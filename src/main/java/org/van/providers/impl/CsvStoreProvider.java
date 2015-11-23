package org.van.providers.impl;

import com.amazonaws.services.simpledb.model.Item;
import org.apache.commons.io.IOUtils;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import org.van.providers.ItemStoreProvider;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Objects;

/**
 * Implementation of {@link ItemStoreProvider} backed by a CSV file.
 *
 * Created by vly on 11/15/2015.
 */
public class CsvStoreProvider extends ItemStoreProvider {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final String HEADERS[] = {"ItemName","AttribName","AttribValue"};

    private OutputStream outputStream;
    private boolean streamOwnership = false;
    private CsvListWriter writer;

    public CsvStoreProvider(OutputStream outputStream) {
        Objects.requireNonNull(outputStream);
        this.outputStream = outputStream;
    }

    /**
     * Sets the stream ownership of this instance. If true, then this instance will close the
     * input stream it is instantiated with. If false, then it will not.
     *
     * @param streamOwnership true to delegate stream ownership to this instance
     *
     * @return this instnace
     */
    public CsvStoreProvider withStreamOwnership(boolean streamOwnership) {
        this.streamOwnership = streamOwnership;
        return this;
    }

    @Override
    public String toString() {
        return String.format("Output stream %s", outputStream);
    }

    @Override
    public ItemStoreProvider initialize() {
        try {
            writer = new CsvListWriter(new OutputStreamWriter(outputStream, UTF8), CsvPreference.EXCEL_PREFERENCE);
            writer.write(HEADERS);
            return this;
        } catch (Exception ex) {
            throw new RuntimeException("Cannot initialize item store", ex);
        }
    }

    @Override
    public void storeItem(Item item) {
        String itemName = item.getName();
        item.getAttributes().forEach(attr -> {
            try {
                writer.write(itemName, attr.getName(), attr.getValue());
            } catch (IOException e) {
                throw new RuntimeException(String.format("Cannot serialize item %s", itemName), e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(writer);
        if (streamOwnership) {
            IOUtils.closeQuietly(outputStream);
        }
    }
}
