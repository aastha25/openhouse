package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.tables.client.api.ViewApi;
import com.linkedin.openhouse.tables.client.model.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.client.model.GetViewResponseBody;
import com.linkedin.openhouse.tables.client.model.ViewRepresentation;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewVersion;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/**
 * OpenHouse {@link org.apache.iceberg.view.ViewOperations}, active only when {@code
 * spark.sql.catalog.<name>.iceberg-views-enabled=true}. Extends {@link BaseViewOperations} for the
 * {@code base == current} commit protocol and {@code current()}/{@code refresh()} caching, and
 * implements the catalog-specific {@link #doCommit} / {@link #doRefresh} against the OpenHouse
 * Views service through the generated {@link ViewApi}, mirroring how tables use {@code TableApi} in
 * {@link OpenHouseTableOperations}.
 *
 * <p>{@link #doCommit} sends the view definition ({@link CreateUpdateViewRequestBody}) to the
 * service, which authors the {@code metadata.json} and owns its location; {@link #doRefresh} reads
 * the pointer ({@link GetViewResponseBody}) and loads that server-written metadata via {@code
 * refreshFromMetadataLocation(...)}. The service-side create/read endpoints return {@code 404}
 * until the Views service is implemented, so these operations are not yet end-to-end.
 */
@Builder
@Slf4j
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class OpenHouseViewOperations extends BaseViewOperations {

  @Getter(AccessLevel.PROTECTED)
  private TableIdentifier viewIdentifier;

  @Getter(AccessLevel.PROTECTED)
  private FileIO fileIO;

  @Getter(AccessLevel.PROTECTED)
  private ViewApi viewApi;

  @Override
  protected void doRefresh() {
    log.info("Calling doRefresh for view: {}", viewName());
    String databaseId = viewIdentifier.namespace().toString();
    String viewId = viewIdentifier.name();
    Optional<GetViewResponseBody> viewResponse =
        viewApi
            .getViewV1(databaseId, viewId)
            /*
             on 404 from the views service, resume the stream as an empty response (view absent);
             for any other error, surface it with the server message attached.
            */
            .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.empty())
            .onErrorResume(
                WebClientResponseException.class,
                e -> Mono.error(new WebClientResponseWithMessageException(e)))
            .blockOptional();
    Optional<String> metadataLocation = viewResponse.map(GetViewResponseBody::getMetadataLocation);
    if (!metadataLocation.isPresent()) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchViewException(
            "Cannot find view %s after refresh, maybe another process deleted it", viewName());
      }
      // The view does not exist yet (e.g. probed before create); nothing to load.
      return;
    }
    refreshFromMetadataLocation(metadataLocation.get());
    // Potential TODO: check if default value support needs to be added
    log.debug("Calling doRefresh succeeded for view: {}", viewName());
  }

  @Override
  protected void doCommit(ViewMetadata base, ViewMetadata metadata) {
    log.info("Calling doCommit for view: {}", viewName());
    String databaseId = viewIdentifier.namespace().toString();
    String viewId = viewIdentifier.name();
    CreateUpdateViewRequestBody requestBody = constructRequestBody(base, metadata);
    try {
      // base == null is a create (CREATE VIEW); base != null is a replace (CREATE OR REPLACE).
      Mono<GetViewResponseBody> request =
          base == null
              ? viewApi.createViewV1(databaseId, requestBody)
              : viewApi.updateViewV1(databaseId, viewId, requestBody);
      request
          .onErrorResume(
              WebClientResponseException.class,
              e -> Mono.error(new WebClientResponseWithMessageException(e)))
          .block();
    } catch (RuntimeException e) {
      if (e.getCause() instanceof InterruptedException) {
        log.error("Interrupted during doCommit for view: {}", viewName(), e);
        throw new CommitStateUnknownException(e);
      }
      throw e;
    }
    log.debug("Calling doCommit succeeded for view: {}", viewName());
  }

  /**
   * Maps an Iceberg {@link ViewMetadata} to an OpenHouse {@link CreateUpdateViewRequestBody}. The
   * service owns the {@code metadata.json}, so only the view definition is sent. {@code
   * sourceDialect} has no native Iceberg slot; it is derived from the single SQL representation's
   * dialect (M1 supports exactly one representation). {@code baseMetadataLocation} is sent only on
   * update ({@code base != null}) to drive the service's optimistic-concurrency check.
   */
  private CreateUpdateViewRequestBody constructRequestBody(
      ViewMetadata base, ViewMetadata metadata) {
    ViewVersion currentVersion = metadata.currentVersion();
    List<SQLViewRepresentation> sqlRepresentations =
        currentVersion.representations().stream()
            .filter(SQLViewRepresentation.class::isInstance)
            .map(SQLViewRepresentation.class::cast)
            .collect(Collectors.toList());
    // TODO: the current ViewApi design derives sourceDialect from one SQL representation, so a view
    // is tied to a single dialect/engine. We need to support a view that carries multiple
    // representations (e.g. Spark and Trino), where sourceDialect is the immutable edit-owner that
    // decides which engine may later update the view. Until then, reject anything but one
    // representation.
    if (sqlRepresentations.size() != 1) {
      throw new UnsupportedOperationException(
          String.format(
              "OpenHouse views support exactly one SQL representation; found %d for view %s",
              sqlRepresentations.size(), viewName()));
    }
    List<ViewRepresentation> representations =
        sqlRepresentations.stream()
            .map(
                rep ->
                    new ViewRepresentation().type(rep.type()).sql(rep.sql()).dialect(rep.dialect()))
            .collect(Collectors.toList());

    // TODO: the view body carries no clusterId, so the service can't confirm the request's target
    // cluster. Add clusterId and assert it equals the serving cluster server-side, so a client
    // pointed at the wrong fabric is rejected instead of silently creating the view there.
    CreateUpdateViewRequestBody requestBody =
        new CreateUpdateViewRequestBody()
            .viewId(viewIdentifier.name())
            .databaseId(viewIdentifier.namespace().toString())
            .schema(SchemaParser.toJson(metadata.schema(), false))
            .representations(representations)
            // TODO: sourceDialect is an OpenHouse-only top-level field; neither the Iceberg view
            // spec nor IRC defines it (dialect lives per-representation), so it cannot round-trip
            // through standard Iceberg view metadata and has to land in the properties map anyway.
            // Better outcome: model it as a server-managed, reserved view property (e.g.
            // "openhouse.view.sourceDialect") that the client sets once at create and the server
            // then
            // freezes (reject edits like a preserved key). That keeps the persisted view fully
            // spec-compliant and parseable by any Iceberg/IRC reader, which an extra top-level
            // field is not.
            .sourceDialect(sqlRepresentations.get(0).dialect());

    // defaultCatalog/defaultNamespace mirror the Iceberg view spec's resolution context (the
    // "current" catalog/namespace used to resolve *unqualified* identifiers in the view SQL). Both
    // are optional on the wire; we pass them through only to round-trip a faithful ViewMetadata.
    // When the persisted view SQL is fully qualified, these defaults are never consulted at
    // resolution time. Also, deferred decision, to check if engines want to populate default
    // catalog
    // or OH overrides ViewBuilder to setup default value.
    // Optional fields: set only when present so each JsonNullable stays "undefined" otherwise.
    if (currentVersion.defaultCatalog() != null) {
      requestBody.defaultCatalog(currentVersion.defaultCatalog());
    }
    Namespace defaultNamespace = currentVersion.defaultNamespace();
    if (defaultNamespace != null && !defaultNamespace.isEmpty()) {
      requestBody.defaultNamespace(Arrays.asList(defaultNamespace.levels()));
    }
    Map<String, String> viewProperties = metadata.properties();
    if (viewProperties != null && !viewProperties.isEmpty()) {
      requestBody.viewProperties(viewProperties);
    }
    if (base != null) {
      requestBody.baseMetadataLocation(base.metadataFileLocation());
    }
    return requestBody;
  }

  @Override
  protected String viewName() {
    return viewIdentifier.toString();
  }

  @Override
  protected FileIO io() {
    return fileIO;
  }
}
