package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author huangzihe
 * @date 2023/5/17 7:53 AM
 */
public class LRUCache extends LinkedHashMap<PageId,Page> {

    int maximum;

    public LRUCache(int maximum) {
        super(maximum, 0.75f, true);
        this.maximum = maximum;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<PageId, Page> eldest) {
        boolean res =  size() > maximum;
        if (res) {
            Page page =  eldest.getValue();
            if ((page.isDirty() != null)) {
                for (Page value : Database.getBufferPool().cache.values()) {
                    if (value.isDirty() == null) {
                        Database.getBufferPool().discardPage(value.getId());
                    }
                }
                throw new DbException("all pages are dirty");
            }
        }
        return res;
    }
}
