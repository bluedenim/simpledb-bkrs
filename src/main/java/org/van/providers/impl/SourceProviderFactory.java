package org.van.providers.impl;

import org.van.providers.ItemSourceProvider;
import org.van.providers.ItemStoreProvider;

import java.io.*;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/**
 * Creates {@link org.van.providers.ItemSourceProvider} or {@link org.van.providers.ItemStoreProvider}
 * instances based on their corresponding URIs.
 * <br/>
 * Examples:
 * <br/>
 * <ul>
 *     <li>sdb:///DomainName -- SimpleDB domain "DomainName"</li>
 *     <li>file:///home/van/abc.csv -- file "/home/van/abc.csv" on local disk</li>
 *     <li>file:///c:/My Documents/abc.csv -- file "C:\My Documents\abc.csv" on Windows local disk</li>
 * </ul>
 *
 * Created by vly on 11/22/2015.
 */
public class SourceProviderFactory {

    public static final String SCHEME_FILE = "file";
    public static final String SCHEME_SIMPLEDB = "sdb";

    /**
     * Create a instance of a source provider for the URL provided. Caller should call
     * {@link ItemSourceProvider#initialize()} on the returned instance before using it.
     *
     * @param uri the item source URI
     *
     * @return an {@link ItemSourceProvider} implementation corresponding to the URL provided
     * @throws IllegalArgumentException if the URL uses a scheme we don't support
     * @throws IOException if the URL cannot be accessed
     */
    public static ItemSourceProvider sourceProviderFor(final URI uri) throws IllegalArgumentException, IOException {
        Objects.requireNonNull(uri);
        ItemSourceProvider provider = null;
        String scheme = uri.getScheme();
        switch(scheme) {
            case SCHEME_FILE:
                File file = new File(cleansePath(uri.getPath()));
                provider = new CsvSourceProvider(new FileInputStream(file)).withStreamOwnership(true);
                break;
            case SCHEME_SIMPLEDB:
                String path = cleansePath(uri.getPath());
                String query = String.format("select * from `%s`", path);
                provider = new SimpleDbSourceProvider(Optional.empty(), query, Optional.empty());
                break;
            default:
                throw new IllegalArgumentException(String.format("Scheme %s not supported", scheme));
        }
        return provider;
    }

    /**
     * Create a instance of a store provider for the URL provided. Caller should call
     * {@link ItemStoreProvider#initialize()} on the returned instance before using it.
     *
     * @param uri the URI to create a store provider for
     *
     * @return an {@link ItemStoreProvider} implementation corresponding to the URL provided
     * @throws FileNotFoundException
     */
    public static ItemStoreProvider storeProviderFor(final URI uri) throws FileNotFoundException {
        Objects.requireNonNull(uri);
        ItemStoreProvider provider = null;
        String scheme = uri.getScheme();
        switch(scheme) {
            case SCHEME_FILE:
                String file = cleansePath(uri.getPath());
                provider = new CsvStoreProvider(new FileOutputStream(file));
                break;
            case SCHEME_SIMPLEDB:
                provider = new SimpleDbStoreProvider(Optional.empty(), cleansePath(uri.getPath()));
                break;
            default:
                throw new IllegalArgumentException(String.format("Scheme %s not supported", scheme));
        }
        return provider;
    }

    static String cleansePath(final String path) {
        Objects.requireNonNull(path);
        String answer = path;
        if (answer.startsWith("/")) {
            answer = answer.substring(1);
        }
        return answer;
    }
}
