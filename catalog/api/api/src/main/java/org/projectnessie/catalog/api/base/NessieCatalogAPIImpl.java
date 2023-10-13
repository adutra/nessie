/*
 * Copyright (C) 2023 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.catalog.api.base;

import static io.smallrye.config.PropertiesConfigSourceProvider.classPathSources;
import static io.smallrye.config.PropertiesConfigSourceProvider.propertiesSources;
import static java.lang.Thread.currentThread;

import io.smallrye.config.DotEnvConfigSourceProvider;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.smallrye.config.source.yaml.YamlConfigSource;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import org.projectnessie.catalog.api.base.stub.CatalogStub;
import org.projectnessie.catalog.api.base.transport.CatalogTransport;
import org.projectnessie.catalog.api.base.transport.CatalogTransportFactory;
import org.projectnessie.catalog.model.NessieCatalog;

final class NessieCatalogAPIImpl implements NessieCatalogAPI {

  private static final int DEFAULT_ORDINAL = 500;

  static BuilderImpl builder() {
    return new BuilderImpl();
  }

  static final class BuilderImpl implements NessieCatalogAPI.Builder {

    private final List<Consumer<SmallRyeConfigBuilder>> configConfig = new ArrayList<>();

    @Override
    public Builder withPropertiesSource(String location) {
      configConfig.add(
          configBuilder ->
              configBuilder.withSources(
                  propertiesSources(location, currentThread().getContextClassLoader())));
      return this;
    }

    @Override
    public Builder withClassPathSource(String location) {
      configConfig.add(
          configBuilder ->
              configBuilder.withSources(
                  classPathSources(location, currentThread().getContextClassLoader())));
      return this;
    }

    @Override
    public Builder withProperties(Properties properties, String sourceName) {
      configConfig.add(
          configBuilder ->
              configBuilder.withSources(new PropertiesConfigSource(properties, sourceName)));
      return this;
    }

    @Override
    public Builder withProperties(Map<String, String> propertiesMap, String sourceName) {
      return withProperties(propertiesMap, sourceName, DEFAULT_ORDINAL);
    }

    @Override
    public Builder withProperties(
        Map<String, String> propertiesMap, String sourceName, int ordinal) {
      configConfig.add(
          configBuilder ->
              configBuilder.withSources(
                  new PropertiesConfigSource(propertiesMap, sourceName, ordinal)));
      return this;
    }

    @Override
    public Builder withYamlSource(Path source) {
      return withYamlSource(source, DEFAULT_ORDINAL);
    }

    @Override
    public Builder withYamlSource(Path source, int ordinal) {
      return withYamlSource(uriToURL(source.toUri()), ordinal);
    }

    @Override
    public Builder withYamlSource(URL source) {
      return withYamlSource(source, DEFAULT_ORDINAL);
    }

    @Override
    public Builder withYamlSource(URL source, int ordinal) {
      configConfig.add(
          configBuilder -> configBuilder.withSources(yamlConfigSource(source, ordinal)));
      return this;
    }

    @Override
    public NessieCatalogAPI build() {
      return new NessieCatalogAPIImpl(this);
    }
  }

  private static URL uriToURL(URI source) {
    try {
      return source.toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  private static YamlConfigSource yamlConfigSource(URL source, int ordinal) {
    try {
      return new YamlConfigSource(source, ordinal);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final BuilderImpl builder;

  NessieCatalogAPIImpl(BuilderImpl builder) {
    this.builder = builder;
  }

  private SmallRyeConfigBuilder configBuilder() {
    SmallRyeConfigBuilder configBuilder =
        new SmallRyeConfigBuilder()
            .addDefaultSources()
            .withSources(new DotEnvConfigSourceProvider())
            .withSources(
                classPathSources(
                    "META-INF/nessie-catalog.properties", currentThread().getContextClassLoader()));
    Path configNessie = Paths.get(System.getProperty("user.home"), ".config", "nessie");
    ifFileExists(
        configNessie.resolve("nessie-catalog.properties"),
        f ->
            configBuilder.withSources(
                propertiesSources(f.toString(), currentThread().getContextClassLoader())));
    ifFileExists(
        configNessie.resolve("nessie-catalog.yaml"),
        f -> configBuilder.withSources(yamlConfigSource(uriToURL(f), DEFAULT_ORDINAL)));
    configBuilder.withSources(
        propertiesSources("nessie-catalog.properties", currentThread().getContextClassLoader()));
    ifFileExists(
        Paths.get("nessie-catalog.yaml"),
        f -> configBuilder.withSources(yamlConfigSource(uriToURL(f), DEFAULT_ORDINAL)));
    configBuilder
        .addDiscoveredSources()
        .addDefaultInterceptors()
        .addDiscoveredConverters()
        .addDiscoveredInterceptors()
        .addDiscoveredValidator()
        .withSecretKeys();
    builder.configConfig.forEach(c -> c.accept(configBuilder));
    return configBuilder;
  }

  private static void ifFileExists(Path file, Consumer<URI> consumer) {
    if (Files.isRegularFile(file)) {
      consumer.accept(file.toUri());
    }
  }

  public NessieCatalog connect() {
    NessieCatalogClientConfig.Transport configTransport = configTransport();

    CatalogTransportFactory<Object> factory = catalogTransportFactory(configTransport);
    Optional<Class<Object>> transportConfigType = factory.configMappingType();

    SmallRyeConfig fullConfig = fullConfig(transportConfigType);

    NessieCatalogClientConfig config = fullConfig.getConfigMapping(NessieCatalogClientConfig.class);
    Optional<Object> transportConfig = transportConfigType.map(fullConfig::getConfigMapping);

    CatalogTransport transport = factory.createTransport(config, transportConfig);

    return new CatalogStub(transport);
  }

  private NessieCatalogClientConfig.Transport configTransport() {
    return configBuilder()
        .withMapping(NessieCatalogClientConfig.Transport.class)
        .build()
        .getConfigMapping(NessieCatalogClientConfig.Transport.class);
  }

  private SmallRyeConfig fullConfig(Optional<Class<Object>> transportConfigType) {
    SmallRyeConfigBuilder fullConfigBuilder =
        configBuilder().withMapping(NessieCatalogClientConfig.class);
    transportConfigType.ifPresent(fullConfigBuilder::withMapping);
    return fullConfigBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private static CatalogTransportFactory<Object> catalogTransportFactory(
      NessieCatalogClientConfig.Transport transportConfig) {
    return transportConfig
        .type()
        .map(NessieCatalogAPIImpl::instantiateFactory)
        .orElseGet(() -> lookupTransportFactory(transportConfig));
  }

  @SuppressWarnings("rawtypes")
  private static CatalogTransportFactory instantiateFactory(
      Class<? extends CatalogTransportFactory> factoryType) {
    try {
      return factoryType.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(
          "Could not instantiate Nessie Catalog transport factory " + factoryType, e);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static CatalogTransportFactory<Object> lookupTransportFactory(
      NessieCatalogClientConfig.Transport transportConfig) {
    ServiceLoader<CatalogTransportFactory> transportFactories =
        ServiceLoader.load(CatalogTransportFactory.class);
    for (CatalogTransportFactory factory : transportFactories) {
      if (factory.name().equals(transportConfig.name())) {
        return factory;
      }
    }
    throw new IllegalStateException(
        "Nessie Catalog transport with name '"
            + transportConfig.name()
            + "' not found, missing a dependency?");
  }
}
