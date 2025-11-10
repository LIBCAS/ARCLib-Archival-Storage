package cz.cas.lib.arcstorage.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.nio.file.Paths;

@Component
public class FileLocationResolver {

    private Path tmpFolder;

    /**
     * @param fileId is not always {@link cz.cas.lib.arcstorage.domain.entity.ArchivalObject#id} ... it might be as well
     *               {@link cz.cas.lib.arcstorage.dto.ObjectRetrievalResource#id} or so.. you have to check the usage
     * @return path
     */
    public Path getFileTmpPath(String fileId) {
        return tmpFolder.resolve(fileId);
    }

    @Autowired
    public void setTmpFolder(@Value("${spring.servlet.multipart.location}") String path) {
        this.tmpFolder = Paths.get(path);
    }
}
