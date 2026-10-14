package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.View;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Spark-3.5-only test for the ENABLED OpenHouse view path (the feature's happy path), which cannot
 * run on Spark 3.1 because its Iceberg 1.2 catalog is not a {@code ViewCatalog}. The cross-version
 * disabled-parity behavior is covered by {@link OpenHouseViewSparkITest}.
 *
 * <p>Views are enabled via {@code spark.sql.catalog.openhouse.iceberg-views-enabled=true}. The view
 * operations are exercised programmatically against the Iceberg {@link ViewCatalog}, validating the
 * buildView -> loadView -> listViews -> dropView round-trip.
 *
 * <p><b>Disabled:</b> the client view operations now call the OpenHouse Views service (via {@code
 * ViewApi}) instead of an in-memory store, but the service is currently a 404 stub ({@code
 * ViewsDisabledService}). This end-to-end round-trip therefore cannot pass until the Views service
 * is implemented; re-enable it then. Client-level coverage (mocked {@code ViewApi}) lives in {@code
 * OpenHouseViewOperationsTest} in the iceberg-1.5 itest module.
 */
public class OpenHouseViewEnabledTestSpark3_5 extends OpenHouseSparkITest {

  @Disabled(
      "Requires the OpenHouse Views service, which is currently a 404 stub (ViewsDisabledService). "
          + "Client view ops now call the service via ViewApi; re-enable once the service exists. "
          + "Client-level coverage: OpenHouseViewOperationsTest (iceberg-1.5 itest).")
  @Test
  public void testEnabledViewBuildLoadListDropRoundTrip() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("spark.sql.catalog.openhouse.iceberg-views-enabled", "true");
    try (SparkSession spark = getSparkSession("openhouse", overrides)) {
      ViewCatalog catalog = (ViewCatalog) getOpenHouseCatalog(spark);
      Namespace namespace = Namespace.of("viewenabled_db");
      TableIdentifier viewId = TableIdentifier.of("viewenabled_db", "v_roundtrip");
      Schema schema = new Schema(Types.NestedField.required(1, "c", Types.IntegerType.get()));

      View created =
          catalog
              .buildView(viewId)
              .withSchema(schema)
              .withQuery("spark", "SELECT 1 AS c")
              .withDefaultNamespace(namespace)
              .withDefaultCatalog("openhouse")
              .create();
      assertNotNull(created);

      // loadView round-trip returns the persisted (mock) SQL representation.
      View loaded = catalog.loadView(viewId);
      assertEquals("SELECT 1 AS c", loaded.sqlFor("spark").sql());

      // listViews surfaces the created view within its namespace.
      assertTrue(catalog.listViews(namespace).contains(viewId));

      // dropView removes it, after which loadView reports NoSuchViewException.
      assertTrue(catalog.dropView(viewId));
      assertThrows(NoSuchViewException.class, () -> catalog.loadView(viewId));
    }
  }
}
