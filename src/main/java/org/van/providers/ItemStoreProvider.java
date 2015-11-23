package org.van.providers;

import com.amazonaws.services.simpledb.model.Item;

import java.io.Closeable;
import java.io.IOException;

/**
 * Abstract class for a store of {@link Item}. Users should call {@link #initialize()} before
 * using the instance, call {@link #storeItem(Item)} to add items, and
 * lastly call {@link #close()} when done with the instance.
 *
 * Created by vly on 11/15/2015.
 */
public abstract class ItemStoreProvider implements Closeable {

    /**
     * Initialize the instance. Call this before using this instance.
     *
     * @return this instance
     */
    public ItemStoreProvider initialize() {
        return this;
    }

    /**
     * Store and item into this store.
     *
     * @param item item to add to the store
     */
    public abstract void storeItem(Item item);

    /**
     * Clean up and close any resources.
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
    }
}