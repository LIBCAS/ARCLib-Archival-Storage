package cz.cas.lib.arcstorage.api.multipart;


import jakarta.servlet.http.HttpServletRequest;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.multipart.MultipartHttpServletRequest;
import org.springframework.web.multipart.support.StandardServletMultipartResolver;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ArcstorageMultipartResolver extends StandardServletMultipartResolver {

    private Integer tmpFolderUploadSizeLimitMb;
    private Path tmpFolder;

    @Override
    public MultipartHttpServletRequest resolveMultipart(final HttpServletRequest request) throws MultipartException {
        if (tmpFolderUploadSizeLimitMb != null &&
                FileUtils.sizeOfDirectoryAsBigInteger(tmpFolder.toFile()).compareTo(BigInteger.valueOf(tmpFolderUploadSizeLimitMb * 1000)) > 0
        )
            throw new TmpFolderSizeLimitReachedException();
        return super.resolveMultipart(request);
    }

    @Autowired
    public void setTmpFolderUploadSizeLimitMb(@Value("${arcstorage.tmpFolderUploadSizeLimit:#{null}}") Integer tmpFolderUploadSizeLimitMb) {
        this.tmpFolderUploadSizeLimitMb = tmpFolderUploadSizeLimitMb;
    }

    @Autowired
    public void setTmpFolder(@Value("${spring.servlet.multipart.location}") String path) {
        this.tmpFolder = Paths.get(path);
    }
}
