package org.apache.gora.accumulo;

import org.apache.avro.util.Utf8;
import org.apache.gora.accumulo.store.AccumuloStore;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.WebPage;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class Gora419Test {

    private static final String KEY = "key";
    private static final String OTHER_FIELD_VALUE = "batchId";
    private static final String MAP_KEY = "foo";
    private static final String MAP_VALUE = "bar";

    @Test
    public void testThatMapUpdateKeepsOtherColumns() throws Exception {
        Configuration conf = new Configuration();
        DataStore<String, WebPage> store = createStore(conf, String.class, WebPage.class);

        WebPage w1 = new WebPage();
        w1.setBatchId(OTHER_FIELD_VALUE);
        store.put(KEY, w1);
        store.flush();

        WebPage w2 = store.get(KEY, new String[] { WebPage.Field.BATCH_ID.getName() });
        assertThat("batch id stored correctly", w2.getBatchId().toString(), is(OTHER_FIELD_VALUE));
        w2.getHeaders().put(MAP_KEY, MAP_VALUE);
        store.put(KEY, w2);
        store.flush();

        WebPage read = store.get(KEY);
        assertThat("webpage found", read, is(notNullValue()));
        assertThat("batch id still stored", read.getBatchId(), is(notNullValue()));
        assertThat("batch id has correct value", read.getBatchId().toString(),
            is(OTHER_FIELD_VALUE));
        assertThat("map value stored correctly",
            read.getHeaders().get(new Utf8(MAP_KEY)).toString(),
            is(MAP_VALUE));
    }

    private <K, V extends Persistent> DataStore<K, V> createStore(Configuration conf,
            Class<K> keyClass, Class<V> valueClass) throws GoraException {
        @SuppressWarnings("unchecked")
        Class<? extends DataStore<K, V>> dataStoreClass = (Class<? extends DataStore<K, V>>) AccumuloStore.class;
        return DataStoreFactory.createDataStore(dataStoreClass, keyClass, valueClass, conf);
    }

}
