package org.van.providers;

import com.amazonaws.services.simpledb.model.Item;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by vly on 11/15/2015.
 */
public abstract class ItemStoreProvider implements Closeable {

    public ItemStoreProvider initialize() {
        return this;
    }

    public abstract void storeItem(Item item);

    @Override
    public void close() throws IOException {
    }
}