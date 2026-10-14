package com.linkedin.openhouse.javaclient;

import static org.mockito.Mockito.*;

import com.linkedin.openhouse.gen.tables.client.api.ViewApi;
import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.gen.tables.client.model.GetViewResponseBody;
import com.linkedin.openhouse.gen.tables.client.model.ViewRepresentation;
import com.linkedin.openhouse.relocated.reactor.core.publisher.Mono;
import java.util.Arrays;
import java.util.Collections;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link OpenHouseViewOperations}, the iceberg-1.5-only, {@link ViewApi}-backed view
 * operations. The tests mock {@link ViewApi} plus the Iceberg {@link ViewMetadata}/{@link
 * ViewVersion}/{@link SQLViewRepresentation} interfaces to exercise the {@code doCommit} create /
 * replace mapping and the {@code doRefresh} not-found handling in isolation, without a running
 * service. Mirrors the conventions of {@code OpenHouseTableOperationsTest}.
 */
public class OpenHouseViewOperationsTest {

  private OpenHouseViewOperations viewOps(ViewApi viewApi) {
    return OpenHouseViewOperations.builder()
        .viewIdentifier(TableIdentifier.of("db", "v"))
        .fileIO(mock(FileIO.class))
        .viewApi(viewApi)
        .build();
  }

  /** A {@link ViewMetadata} carrying exactly one SQL representation with the given dialect. */
  private ViewMetadata singleSqlViewMetadata(String dialect) {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    SQLViewRepresentation sqlRep = mock(SQLViewRepresentation.class);
    when(sqlRep.type()).thenReturn("sql");
    when(sqlRep.sql()).thenReturn("SELECT 1");
    when(sqlRep.dialect()).thenReturn(dialect);
    org.apache.iceberg.view.ViewRepresentation repAsBase = sqlRep;

    ViewVersion version = mock(ViewVersion.class);
    when(version.representations()).thenReturn(Collections.singletonList(repAsBase));
    when(version.defaultCatalog()).thenReturn("openhouse");
    when(version.defaultNamespace()).thenReturn(Namespace.of("db"));

    ViewMetadata metadata = mock(ViewMetadata.class);
    when(metadata.currentVersion()).thenReturn(version);
    when(metadata.schema()).thenReturn(schema);
    when(metadata.properties()).thenReturn(Collections.singletonMap("comment", "hi"));
    return metadata;
  }

  @Test
  public void testDoCommitCreateMapsFieldsAndCallsCreateView() {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.createViewV1(anyString(), any())).thenReturn(Mono.just(new GetViewResponseBody()));
    OpenHouseViewOperations ops = viewOps(viewApi);

    ops.doCommit(null, singleSqlViewMetadata("spark"));

    ArgumentCaptor<CreateUpdateViewRequestBody> captor =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    verify(viewApi).createViewV1(eq("db"), captor.capture());
    verify(viewApi, never()).updateViewV1(anyString(), anyString(), any());

    CreateUpdateViewRequestBody body = captor.getValue();
    Assertions.assertEquals("v", body.getViewId());
    Assertions.assertEquals("db", body.getDatabaseId());
    Assertions.assertTrue(body.getSchema().contains("\"id\""));
    Assertions.assertEquals(1, body.getRepresentations().size());
    ViewRepresentation rep = body.getRepresentations().get(0);
    Assertions.assertEquals("sql", rep.getType());
    Assertions.assertEquals("SELECT 1", rep.getSql());
    Assertions.assertEquals("spark", rep.getDialect());
    // sourceDialect has no native Iceberg slot; it is derived from the single representation.
    Assertions.assertEquals("spark", body.getSourceDialect());
    Assertions.assertEquals("openhouse", body.getDefaultCatalog());
    Assertions.assertEquals(Collections.singletonList("db"), body.getDefaultNamespace());
    Assertions.assertEquals("hi", body.getViewProperties().get("comment"));
    // No optimistic-concurrency token is sent on create.
    Assertions.assertNull(body.getBaseMetadataLocation());
  }

  @Test
  public void testDoCommitReplaceSetsBaseLocationAndCallsUpdateView() {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.updateViewV1(anyString(), anyString(), any()))
        .thenReturn(Mono.just(new GetViewResponseBody()));
    OpenHouseViewOperations ops = viewOps(viewApi);

    ViewMetadata base = mock(ViewMetadata.class);
    when(base.metadataFileLocation()).thenReturn("s3://bucket/db/v/old-metadata.json");

    ops.doCommit(base, singleSqlViewMetadata("spark"));

    ArgumentCaptor<CreateUpdateViewRequestBody> captor =
        ArgumentCaptor.forClass(CreateUpdateViewRequestBody.class);
    verify(viewApi).updateViewV1(eq("db"), eq("v"), captor.capture());
    verify(viewApi, never()).createViewV1(anyString(), any());
    // On replace, the base metadata location drives the service's optimistic-concurrency check.
    Assertions.assertEquals(
        "s3://bucket/db/v/old-metadata.json", captor.getValue().getBaseMetadataLocation());
  }

  @Test
  public void testDoCommitMultipleRepresentationsThrows() {
    ViewApi viewApi = mock(ViewApi.class);
    OpenHouseViewOperations ops = viewOps(viewApi);

    org.apache.iceberg.view.ViewRepresentation rep1 = mock(SQLViewRepresentation.class);
    org.apache.iceberg.view.ViewRepresentation rep2 = mock(SQLViewRepresentation.class);
    ViewVersion version = mock(ViewVersion.class);
    when(version.representations()).thenReturn(Arrays.asList(rep1, rep2));
    ViewMetadata metadata = mock(ViewMetadata.class);
    when(metadata.currentVersion()).thenReturn(version);

    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> ops.doCommit(null, metadata));
    verify(viewApi, never()).createViewV1(anyString(), any());
    verify(viewApi, never()).updateViewV1(anyString(), anyString(), any());
  }

  @Test
  public void testDoRefreshViewNotFoundDoesNotThrow() {
    ViewApi viewApi = mock(ViewApi.class);
    when(viewApi.getViewV1("db", "v")).thenReturn(Mono.empty());
    OpenHouseViewOperations ops = viewOps(viewApi);

    // Absent view with no current metadata location (never committed) is a no-op, not an error.
    Assertions.assertDoesNotThrow(ops::doRefresh);
    verify(viewApi).getViewV1("db", "v");
  }
}
