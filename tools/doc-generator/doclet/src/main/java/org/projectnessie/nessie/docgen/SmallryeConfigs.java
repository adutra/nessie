/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.docgen;

import static java.util.Arrays.asList;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.ConfigMappingInterface;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.AbstractElementVisitor8;
import javax.tools.StandardLocation;
import jdk.javadoc.doclet.DocletEnvironment;

public class SmallryeConfigs {
  private final DocletEnvironment env;

  private final ClassLoader classLoader;

  private final Map<String, SmallRyeConfigMappingInfo> configMappingInfos = new HashMap<>();
  private final Map<String, SmallRyeConfigMappingInfo> configMappingByType = new HashMap<>();

  public SmallryeConfigs(DocletEnvironment env) {
    this.classLoader = env.getJavaFileManager().getClassLoader(StandardLocation.CLASS_PATH);
    this.env = env;
  }

  public Collection<SmallRyeConfigMappingInfo> getConfigMappingInfos() {
    return configMappingInfos.values();
  }

  public SmallRyeConfigMappingInfo getConfigMappingInfo(Class<?> type) {
    String typeName = type.getName();
    SmallRyeConfigMappingInfo info = configMappingByType.get(typeName);
    if (info == null) {
      info = new SmallRyeConfigMappingInfo("");
      configMappingByType.put(typeName, info);
      TypeElement elem = env.getElementUtils().getTypeElement(typeName);
      elem.accept(visitor(), null);
    }
    return info;
  }

  ElementVisitor<Void, Void> visitor() {
    return new AbstractElementVisitor8<>() {

      @Override
      public Void visitPackage(PackageElement e, Void ignore) {
        return null;
      }

      @Override
      public Void visitType(TypeElement e, Void ignore) {
        switch (e.getKind()) {
          case CLASS:
          case INTERFACE:
            ConfigMapping configMapping = e.getAnnotation(ConfigMapping.class);
            SmallRyeConfigMappingInfo mappingInfo;
            ConfigMappingInterface configMappingInterface;

            String className = e.getQualifiedName().toString();

            if (configMapping != null) {
              mappingInfo =
                  configMappingInfos.computeIfAbsent(
                      configMapping.prefix(), SmallRyeConfigMappingInfo::new);

              Class<?> clazz = loadClass(className);
              try {
                configMappingInterface = ConfigMappingInterface.getConfigurationInterface(clazz);
              } catch (RuntimeException ex) {
                throw new RuntimeException("Failed to process mapped " + clazz, ex);
              }
              configMappingByType.put(
                  configMappingInterface.getInterfaceType().getName(), mappingInfo);
            } else {
              mappingInfo = configMappingByType.get(className);
              if (mappingInfo == null) {
                return null;
              }
              Class<?> clazz = loadClass(className);
              try {
                configMappingInterface = ConfigMappingInterface.getConfigurationInterface(clazz);
              } catch (RuntimeException ex) {
                throw new RuntimeException("Failed to process implicitly added " + clazz, ex);
              }
            }

            mappingInfo.processType(env, configMappingInterface, e);

            Set<ConfigMappingInterface> seen = new HashSet<>();
            Deque<ConfigMappingInterface> remaining =
                new ArrayDeque<>(asList(configMappingInterface.getSuperTypes()));
            while (!remaining.isEmpty()) {
              ConfigMappingInterface superType = remaining.removeFirst();
              if (!seen.add(superType)) {
                continue;
              }

              remaining.addAll(asList(superType.getSuperTypes()));

              TypeElement superTypeElement =
                  env.getElementUtils().getTypeElement(superType.getInterfaceType().getName());
              mappingInfo.processType(env, superType, superTypeElement);
            }
            break;
          default:
            break;
        }

        return null;
      }

      private Class<?> loadClass(String className) {
        try {
          return Class.forName(className, false, classLoader);
        } catch (ClassNotFoundException ex) {
          throw new RuntimeException(ex);
        }
      }

      @Override
      public Void visitVariable(VariableElement e, Void ignore) {
        return null;
      }

      @Override
      public Void visitExecutable(ExecutableElement e, Void ignore) {
        return null;
      }

      @Override
      public Void visitTypeParameter(TypeParameterElement e, Void ignore) {
        return null;
      }
    };
  }
}
