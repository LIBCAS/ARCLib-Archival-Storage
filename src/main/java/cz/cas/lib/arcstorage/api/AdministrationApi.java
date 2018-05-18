package cz.cas.lib.arcstorage.api;

import cz.cas.lib.arcstorage.domain.entity.Storage;
import cz.cas.lib.arcstorage.domain.store.StorageStore;
import cz.cas.lib.arcstorage.domain.store.Transactional;
import cz.cas.lib.arcstorage.dto.StorageUpdateDto;
import cz.cas.lib.arcstorage.exception.MissingObject;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;
import javax.validation.Valid;
import java.util.Collection;

import static cz.cas.lib.arcstorage.util.Utils.notNull;

@RestController
@RequestMapping("/api/administration")
public class AdministrationApi {

    private StorageStore storageStore;

    @Transactional
    @RequestMapping(value = "/storage", method = RequestMethod.GET)
    public Collection<Storage> getAll() {
        return storageStore.findAll();
    }

    @Transactional
    @RequestMapping(value = "/storage/{id}", method = RequestMethod.GET)
    public Storage getOne(@PathVariable("id") String id) {
        return storageStore.find(id);
    }

    @Transactional
    @RequestMapping(value = "/storage", method = RequestMethod.POST)
    public Storage create(@RequestBody Storage storage) {
        storageStore.save(storage);
        return storage;
    }

    @Transactional
    @RequestMapping(value = "/storage/update", method = RequestMethod.POST)
    public Storage update(@RequestBody @Valid StorageUpdateDto storageUpdateDto) {
        Storage storage = storageStore.find(storageUpdateDto.getId());
        notNull(storage, () -> new MissingObject(Storage.class, storageUpdateDto.getId()));
        storage.setName(storageUpdateDto.getName());
        storage.setPriority(storageUpdateDto.getPriority());
        storage.setNote(storageUpdateDto.getNote());
        storageStore.save(storage);
        return storage;
    }

    @Transactional
    @RequestMapping(value = "/storage/{id}", method = RequestMethod.DELETE)
    public void delete(@PathVariable("id") String id) {
        storageStore.delete(new Storage(id));
    }

    @Inject
    public void setStorageStore(StorageStore storageStore) {
        this.storageStore = storageStore;
    }
}
