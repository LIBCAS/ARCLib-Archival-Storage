package cz.cas.lib.arcstorage.api.multipart;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.FileUpload;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.commons.CommonsMultipartFile;
import org.springframework.web.util.WebUtils;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * copy of {@link org.springframework.web.multipart.commons.CommonsFileUploadSupport}
 */
public abstract class ArcstorageFileUploadSupport {

    protected final Log logger = LogFactory.getLog(getClass());

    private final DiskFileItemFactory fileItemFactory;

    private final FileUpload fileUpload;

    private boolean uploadTempDirSpecified = false;

    private boolean preserveFilename = false;


    /**
     * Instantiate a new CommonsFileUploadSupport with its
     * corresponding FileItemFactory and FileUpload instances.
     *
     * @see #newFileItemFactory
     * @see #newFileUpload
     */
    public ArcstorageFileUploadSupport() {
        this.fileItemFactory = newFileItemFactory();
        this.fileUpload = newFileUpload(getFileItemFactory());
    }


    /**
     * Return the underlying {@code org.apache.commons.fileupload.disk.DiskFileItemFactory}
     * instance. There is hardly any need to access this.
     *
     * @return the underlying DiskFileItemFactory instance
     */
    public DiskFileItemFactory getFileItemFactory() {
        return this.fileItemFactory;
    }

    /**
     * Return the underlying {@code org.apache.commons.fileupload.FileUpload}
     * instance. There is hardly any need to access this.
     *
     * @return the underlying FileUpload instance
     */
    public FileUpload getFileUpload() {
        return this.fileUpload;
    }

    /**
     * Set the maximum allowed size (in bytes) before an upload gets rejected.
     * -1 indicates no limit (the default).
     *
     * @param maxUploadSize the maximum upload size allowed
     * @see org.apache.commons.fileupload.FileUploadBase#setSizeMax
     */
    public void setMaxUploadSize(long maxUploadSize) {
        this.fileUpload.setSizeMax(maxUploadSize);
    }

    /**
     * Set the maximum allowed size (in bytes) for each individual file before
     * an upload gets rejected. -1 indicates no limit (the default).
     *
     * @param maxUploadSizePerFile the maximum upload size per file
     * @see org.apache.commons.fileupload.FileUploadBase#setFileSizeMax
     * @since 4.2
     */
    public void setMaxUploadSizePerFile(long maxUploadSizePerFile) {
        this.fileUpload.setFileSizeMax(maxUploadSizePerFile);
    }

    /**
     * Set the maximum allowed size (in bytes) before uploads are written to disk.
     * Uploaded files will still be received past this amount, but they will not be
     * stored in memory. Default is 10240, according to Commons FileUpload.
     *
     * @param maxInMemorySize the maximum in memory size allowed
     * @see org.apache.commons.fileupload.disk.DiskFileItemFactory#setSizeThreshold
     */
    public void setMaxInMemorySize(int maxInMemorySize) {
        this.fileItemFactory.setSizeThreshold(maxInMemorySize);
    }

    /**
     * Set the default character encoding to use for parsing requests,
     * to be applied to headers of individual parts and to form fields.
     * Default is ISO-8859-1, according to the Servlet spec.
     * <p>If the request specifies a character encoding itself, the request
     * encoding will override this setting. This also allows for generically
     * overriding the character encoding in a filter that invokes the
     * {@code ServletRequest.setCharacterEncoding} method.
     *
     * @param defaultEncoding the character encoding to use
     * @see javax.servlet.ServletRequest#getCharacterEncoding
     * @see javax.servlet.ServletRequest#setCharacterEncoding
     * @see WebUtils#DEFAULT_CHARACTER_ENCODING
     * @see org.apache.commons.fileupload.FileUploadBase#setHeaderEncoding
     */
    public void setDefaultEncoding(String defaultEncoding) {
        this.fileUpload.setHeaderEncoding(defaultEncoding);
    }

    /**
     * Determine the default encoding to use for parsing requests.
     *
     * @see #setDefaultEncoding
     */
    protected String getDefaultEncoding() {
        String encoding = getFileUpload().getHeaderEncoding();
        if (encoding == null) {
            encoding = WebUtils.DEFAULT_CHARACTER_ENCODING;
        }
        return encoding;
    }

    /**
     * Set the temporary directory where uploaded files get stored.
     * Default is the servlet container's temporary directory for the web application.
     *
     * @see org.springframework.web.util.WebUtils#TEMP_DIR_CONTEXT_ATTRIBUTE
     */
    public void setUploadTempDir(Path tmpDir) {
        if (!tmpDir.toFile().exists() && !tmpDir.toFile().mkdirs()) {
            throw new IllegalArgumentException("Given uploadTempDir [" + tmpDir + "] could not be created");
        }
        this.fileItemFactory.setRepository(tmpDir.toFile());
        this.uploadTempDirSpecified = true;
    }

    /**
     * Return the temporary directory where uploaded files get stored.
     *
     * @see #setUploadTempDir
     */
    protected boolean isUploadTempDirSpecified() {
        return this.uploadTempDirSpecified;
    }

    /**
     * Set whether to preserve the filename as sent by the client, not stripping off
     * path information in {@link CommonsMultipartFile#getOriginalFilename()}.
     * <p>Default is "false", stripping off path information that may prefix the
     * actual filename e.g. from Opera. Switch this to "true" for preserving the
     * client-specified filename as-is, including potential path separators.
     *
     * @see MultipartFile#getOriginalFilename()
     * @see CommonsMultipartFile#setPreserveFilename(boolean)
     * @since 4.3.5
     */
    public void setPreserveFilename(boolean preserveFilename) {
        this.preserveFilename = preserveFilename;
    }


    /**
     * Factory method for a Commons DiskFileItemFactory instance.
     * <p>Default implementation returns a standard DiskFileItemFactory.
     * Can be overridden to use a custom subclass, e.g. for testing purposes.
     *
     * @return the new DiskFileItemFactory instance
     */
    protected DiskFileItemFactory newFileItemFactory() {
        return new DiskFileItemFactory();
    }

    /**
     * Factory method for a Commons FileUpload instance.
     * <p><b>To be implemented by subclasses.</b>
     *
     * @param fileItemFactory the Commons FileItemFactory to build upon
     * @return the Commons FileUpload instance
     */
    protected abstract FileUpload newFileUpload(FileItemFactory fileItemFactory);


    /**
     * Determine an appropriate FileUpload instance for the given encoding.
     * <p>Default implementation returns the shared FileUpload instance
     * if the encoding matches, else creates a new FileUpload instance
     * with the same configuration other than the desired encoding.
     *
     * @param encoding the character encoding to use
     * @return an appropriate FileUpload instance.
     */
    protected FileUpload prepareFileUpload(String encoding) {
        FileUpload fileUpload = getFileUpload();
        FileUpload actualFileUpload = fileUpload;

        // Use new temporary FileUpload instance if the request specifies
        // its own encoding that does not match the default encoding.
        if (encoding != null && !encoding.equals(fileUpload.getHeaderEncoding())) {
            actualFileUpload = newFileUpload(getFileItemFactory());
            actualFileUpload.setSizeMax(fileUpload.getSizeMax());
            actualFileUpload.setFileSizeMax(fileUpload.getFileSizeMax());
            actualFileUpload.setHeaderEncoding(encoding);
        }

        return actualFileUpload;
    }

    /**
     * Parse the given List of Commons FileItems into a Spring MultipartParsingResult,
     * containing Spring MultipartFile instances and a Map of multipart parameter.
     *
     * @param fileItems the Commons FileIterms to parse
     * @param encoding  the encoding to use for form fields
     * @return the Spring MultipartParsingResult
     * @see CommonsMultipartFile#CommonsMultipartFile(org.apache.commons.fileupload.FileItem)
     */
    protected MultipartParsingResult parseFileItems(List<FileItem> fileItems, String encoding) {
        MultiValueMap<String, MultipartFile> multipartFiles = new LinkedMultiValueMap<String, MultipartFile>();
        Map<String, String[]> multipartParameters = new HashMap<String, String[]>();
        Map<String, String> multipartParameterContentTypes = new HashMap<String, String>();

        // Extract multipart files and multipart parameters.
        for (FileItem fileItem : fileItems) {
            if (fileItem.isFormField()) {
                String value;
                String partEncoding = determineEncoding(fileItem.getContentType(), encoding);
                if (partEncoding != null) {
                    try {
                        value = fileItem.getString(partEncoding);
                    } catch (UnsupportedEncodingException ex) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Could not decode multipart item '" + fileItem.getFieldName() +
                                    "' with encoding '" + partEncoding + "': using platform default");
                        }
                        value = fileItem.getString();
                    }
                } else {
                    value = fileItem.getString();
                }
                String[] curParam = multipartParameters.get(fileItem.getFieldName());
                if (curParam == null) {
                    // simple form field
                    multipartParameters.put(fileItem.getFieldName(), new String[]{value});
                } else {
                    // array of simple form fields
                    String[] newParam = StringUtils.addStringToArray(curParam, value);
                    multipartParameters.put(fileItem.getFieldName(), newParam);
                }
                multipartParameterContentTypes.put(fileItem.getFieldName(), fileItem.getContentType());
            } else {
                // multipart file field
                CommonsMultipartFile file = createMultipartFile(fileItem);
                multipartFiles.add(file.getName(), file);
//                if (logger.isDebugEnabled()) {
//                    logger.debug("Found multipart file [" + file.getName() + "] of size " + file.getSize() +
//                            " bytes with original filename [" + file.getOriginalFilename() + "], stored " +
//                            file.getStorageDescription());
//                }
            }
        }
        return new MultipartParsingResult(multipartFiles, multipartParameters, multipartParameterContentTypes);
    }

    /**
     * Create a {@link CommonsMultipartFile} wrapper for the given Commons {@link FileItem}.
     *
     * @param fileItem the Commons FileItem to wrap
     * @return the corresponding CommonsMultipartFile (potentially a custom subclass)
     * @see #setPreserveFilename(boolean)
     * @see CommonsMultipartFile#setPreserveFilename(boolean)
     * @since 4.3.5
     */
    protected CommonsMultipartFile createMultipartFile(FileItem fileItem) {
        CommonsMultipartFile multipartFile = new CommonsMultipartFile(fileItem);
        multipartFile.setPreserveFilename(this.preserveFilename);
        return multipartFile;
    }

    /**
     * Cleanup the Spring MultipartFiles created during multipart parsing,
     * potentially holding temporary data on disk.
     * <p>Deletes the underlying Commons FileItem instances.
     *
     * @param multipartFiles Collection of MultipartFile instances
     * @see org.apache.commons.fileupload.FileItem#delete()
     */
    protected void cleanupFileItems(MultiValueMap<String, MultipartFile> multipartFiles) {
        for (List<MultipartFile> files : multipartFiles.values()) {
            for (MultipartFile file : files) {
                if (file instanceof CommonsMultipartFile) {
                    CommonsMultipartFile cmf = (CommonsMultipartFile) file;
                    cmf.getFileItem().delete();
//                    if (logger.isDebugEnabled()) {
//                        logger.debug("Cleaning up multipart file [" + cmf.getName() + "] with original filename [" +
//                                cmf.getOriginalFilename() + "], stored " + cmf.getStorageDescription());
//                    }
                }
            }
        }
    }

    private String determineEncoding(String contentTypeHeader, String defaultEncoding) {
        if (!StringUtils.hasText(contentTypeHeader)) {
            return defaultEncoding;
        }
        MediaType contentType = MediaType.parseMediaType(contentTypeHeader);
        Charset charset = contentType.getCharset();
        return (charset != null ? charset.name() : defaultEncoding);
    }


    /**
     * Holder for a Map of Spring MultipartFiles and a Map of
     * multipart parameters.
     */
    protected static class MultipartParsingResult {

        private final MultiValueMap<String, MultipartFile> multipartFiles;

        private final Map<String, String[]> multipartParameters;

        private final Map<String, String> multipartParameterContentTypes;

        public MultipartParsingResult(MultiValueMap<String, MultipartFile> mpFiles,
                                      Map<String, String[]> mpParams, Map<String, String> mpParamContentTypes) {

            this.multipartFiles = mpFiles;
            this.multipartParameters = mpParams;
            this.multipartParameterContentTypes = mpParamContentTypes;
        }

        public MultiValueMap<String, MultipartFile> getMultipartFiles() {
            return this.multipartFiles;
        }

        public Map<String, String[]> getMultipartParameters() {
            return this.multipartParameters;
        }

        public Map<String, String> getMultipartParameterContentTypes() {
            return this.multipartParameterContentTypes;
        }
    }

}
