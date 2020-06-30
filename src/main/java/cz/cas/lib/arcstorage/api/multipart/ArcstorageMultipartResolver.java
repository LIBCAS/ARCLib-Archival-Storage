package cz.cas.lib.arcstorage.api.multipart;


import org.apache.commons.fileupload.*;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.Assert;
import org.springframework.web.context.ServletContextAware;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.multipart.MultipartHttpServletRequest;
import org.springframework.web.multipart.MultipartResolver;
import org.springframework.web.multipart.commons.CommonsFileUploadSupport;
import org.springframework.web.multipart.support.DefaultMultipartHttpServletRequest;
import org.springframework.web.util.WebUtils;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Copy of {@link org.springframework.web.multipart.commons.CommonsMultipartResolver}, which blocks new multipart uploads if
 * size of arcstorage.tmpFolder grows up to arcstorage.tmpFolderUploadSizeLimit.
 * <br>
 * This copy does not extend {@link CommonsFileUploadSupport}, some of the code was copied into this class though.
 */
public class ArcstorageMultipartResolver extends ArcstorageFileUploadSupport
        implements MultipartResolver, ServletContextAware {

    private boolean resolveLazily = false;
    private Integer tmpFolderUploadSizeLimitMb;
    private Path tmpFolder;

    /**
     * Constructor for use as bean. Determines the servlet container's
     * temporary directory via the ServletContext passed in as through the
     * ServletContextAware interface (typically by a WebApplicationContext).
     *
     * @see #setServletContext
     * @see org.springframework.web.context.ServletContextAware
     * @see org.springframework.web.context.WebApplicationContext
     */
    public ArcstorageMultipartResolver() {
        super();
    }

    /**
     * Constructor for standalone usage. Determines the servlet container's
     * temporary directory via the given ServletContext.
     *
     * @param servletContext the ServletContext to use
     */
    public ArcstorageMultipartResolver(ServletContext servletContext) {
        this();
        setServletContext(servletContext);
    }


    /**
     * Set whether to resolve the multipart request lazily at the time of
     * file or parameter access.
     * <p>Default is "false", resolving the multipart elements immediately, throwing
     * corresponding exceptions at the time of the {@link #resolveMultipart} call.
     * Switch this to "true" for lazy multipart parsing, throwing parse exceptions
     * once the application attempts to obtain multipart files or parameters.
     */
    public void setResolveLazily(boolean resolveLazily) {
        this.resolveLazily = resolveLazily;
    }

    /**
     * Initialize the underlying {@code org.apache.commons.fileupload.servlet.ServletFileUpload}
     * instance. Can be overridden to use a custom subclass, e.g. for testing purposes.
     *
     * @param fileItemFactory the Commons FileItemFactory to use
     * @return the new ServletFileUpload instance
     */
    @Override
    protected FileUpload newFileUpload(FileItemFactory fileItemFactory) {
        ServletFileUpload servletFileUpload = new ServletFileUpload(fileItemFactory);
        return servletFileUpload;
    }

    @Override
    public void setServletContext(ServletContext servletContext) {
        if (!isUploadTempDirSpecified()) {
            getFileItemFactory().setRepository(WebUtils.getTempDir(servletContext));
        }
    }


    @Override
    public boolean isMultipart(HttpServletRequest request) {
        return (request != null && ServletFileUpload.isMultipartContent(request));
    }

    @Override
    public MultipartHttpServletRequest resolveMultipart(final HttpServletRequest request) throws MultipartException {
        Assert.notNull(request, "Request must not be null");
        if (tmpFolderUploadSizeLimitMb != null &&
                FileUtils.sizeOfDirectoryAsBigInteger(tmpFolder.toFile()).compareTo(BigInteger.valueOf(tmpFolderUploadSizeLimitMb * 1000)) > 0
        )
            throw new TmpFolderSizeLimitReachedException();
        if (this.resolveLazily) {
            return new DefaultMultipartHttpServletRequest(request) {
                @Override
                protected void initializeMultipart() {
                    MultipartParsingResult parsingResult = parseRequest(request);
                    setMultipartFiles(parsingResult.getMultipartFiles());
                    setMultipartParameters(parsingResult.getMultipartParameters());
                    setMultipartParameterContentTypes(parsingResult.getMultipartParameterContentTypes());
                }
            };
        } else {
            MultipartParsingResult parsingResult = parseRequest(request);
            return new DefaultMultipartHttpServletRequest(request, parsingResult.getMultipartFiles(),
                    parsingResult.getMultipartParameters(), parsingResult.getMultipartParameterContentTypes());
        }
    }

    /**
     * Parse the given servlet request, resolving its multipart elements.
     *
     * @param request the request to parse
     * @return the parsing result
     * @throws MultipartException if multipart resolution failed.
     */
    protected MultipartParsingResult parseRequest(HttpServletRequest request) throws MultipartException {
        String encoding = determineEncoding(request);
        FileUpload fileUpload = prepareFileUpload(encoding);
        try {
            List<FileItem> fileItems = ((ServletFileUpload) fileUpload).parseRequest(request);
            return parseFileItems(fileItems, encoding);
        } catch (FileUploadBase.SizeLimitExceededException ex) {
            throw new MaxUploadSizeExceededException(fileUpload.getSizeMax(), ex);
        } catch (FileUploadBase.FileSizeLimitExceededException ex) {
            throw new MaxUploadSizeExceededException(fileUpload.getFileSizeMax(), ex);
        } catch (FileUploadException ex) {
            throw new MultipartException("Failed to parse multipart servlet request", ex);
        }
    }

    /**
     * Determine the encoding for the given request.
     * Can be overridden in subclasses.
     * <p>The default implementation checks the request encoding,
     * falling back to the default encoding specified for this resolver.
     *
     * @param request current HTTP request
     * @return the encoding for the request (never {@code null})
     * @see javax.servlet.ServletRequest#getCharacterEncoding
     * @see #setDefaultEncoding
     */
    protected String determineEncoding(HttpServletRequest request) {
        String encoding = request.getCharacterEncoding();
        if (encoding == null) {
            encoding = getDefaultEncoding();
        }
        return encoding;
    }

    @Override
    public void cleanupMultipart(MultipartHttpServletRequest request) {
        if (request != null) {
            try {
                cleanupFileItems(request.getMultiFileMap());
            } catch (Throwable ex) {
                logger.warn("Failed to perform multipart cleanup for servlet request", ex);
            }
        }
    }

    @Inject
    public void setTmpFolderUploadSizeLimitMb(@Value("${arcstorage.tmpFolderUploadSizeLimit:#{null}}") Integer tmpFolderUploadSizeLimitMb) {
        this.tmpFolderUploadSizeLimitMb = tmpFolderUploadSizeLimitMb;
    }

    @Inject
    public void setTmpFolder(@Value("${arcstorage.tmpFolder}") String path) {
        this.tmpFolder = Paths.get(path);
        setUploadTempDir(tmpFolder);
    }
}
