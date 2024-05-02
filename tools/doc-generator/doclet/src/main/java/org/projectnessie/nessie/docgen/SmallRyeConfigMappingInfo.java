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

import com.sun.source.doctree.DocCommentTree;
import io.smallrye.config.ConfigMapping.NamingStrategy;
import io.smallrye.config.ConfigMappingInterface;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.util.AbstractElementVisitor8;
import jdk.javadoc.doclet.DocletEnvironment;

public final class SmallRyeConfigMappingInfo {
  final String prefix;
  final List<ConfigMappingInterface> configMappingInterfaces = new ArrayList<>();
  final Map<String, List<ExecutableElement>> methodExecutables = new HashMap<>();
  final Set<String> propertiesMethodNameOrder = new LinkedHashSet<>();
  final Map<String, ConfigMappingInterface.Property> methodNameToProperty = new LinkedHashMap<>();
  DocCommentTree typeComment;
  TypeElement element;

  SmallRyeConfigMappingInfo(String prefix) {
    this.prefix = prefix;
  }

  public Stream<SmallRyeConfigPropertyInfo> properties(DocletEnvironment env) {
    for (ConfigMappingInterface configMappingInterface : configMappingInterfaces) {
      for (ConfigMappingInterface.Property property : configMappingInterface.getProperties()) {
        String methodName = property.getMethod().getName();
        methodNameToProperty.putIfAbsent(methodName, property);
      }
    }

    return propertiesMethodNameOrder.stream()
        .map(name -> buildPropertyInfo(env, name))
        .filter(Objects::nonNull);
  }

  private SmallRyeConfigPropertyInfo buildPropertyInfo(DocletEnvironment env, String methodName) {
    ConfigMappingInterface.Property property = methodNameToProperty.get(methodName);
    if (property == null) {
      return null;
    }

    DocCommentTree doc = null;

    List<ExecutableElement> executables = methodExecutables.get(methodName);
    if (executables == null) {
      return null;
    }

    ExecutableElement docExec = null;

    for (ExecutableElement executable : executables) {
      if (doc == null) {
        doc = env.getDocTrees().getDocCommentTree(executable);
        if (doc != null) {
          docExec = executable;
        }
      }
    }

    if (docExec == null) {
      docExec = executables.get(0);
    }

    NamingStrategy namingStrategy =
        configMappingInterfaces.isEmpty()
            ? NamingStrategy.KEBAB_CASE
            : configMappingInterfaces.get(0).getNamingStrategy();
    String propertyName = property.getPropertyName(namingStrategy);

    return new SmallRyeConfigPropertyInfo(docExec, property, propertyName, doc);
  }

  void processType(
      DocletEnvironment env,
      ConfigMappingInterface configMappingInterface,
      TypeElement typeElement) {
    configMappingInterfaces.add(configMappingInterface);

    // TODO use the order of the properties as in the source file? or define another annotation?

    // Collect properties defined by the current type-element (type that declares a
    // `@ConfigMapping`)
    for (ConfigMappingInterface.Property property : configMappingInterface.getProperties()) {
      Method method = property.getMethod();
      methodExecutables.putIfAbsent(method.getName(), new ArrayList<>());
    }

    typeElement.accept(executablesVisitor, null);
    if (typeComment == null) {
      typeComment = env.getDocTrees().getDocCommentTree(typeElement);
      if (typeComment != null) {
        this.element = typeElement;
      }
    }
  }

  final ElementVisitor<Void, Void> executablesVisitor =
      new AbstractElementVisitor8<>() {

        @Override
        public Void visitType(TypeElement e, Void unused) {
          for (Element enclosedElement : e.getEnclosedElements()) {
            if (enclosedElement.asType().getKind() == TypeKind.EXECUTABLE) {
              enclosedElement.accept(this, null);
            }
          }
          return null;
        }

        @Override
        public Void visitExecutable(ExecutableElement e, Void unused) {
          if (e.getKind() == ElementKind.METHOD
              && (e.getModifiers().contains(Modifier.ABSTRACT)
                  || e.getModifiers().contains(Modifier.DEFAULT))
              && e.getParameters().isEmpty()
              && e.getReturnType().getKind() != TypeKind.VOID) {
            String methodName = e.getSimpleName().toString();
            propertiesMethodNameOrder.add(methodName);
            List<ExecutableElement> methodList = methodExecutables.get(methodName);
            if (methodList != null) {
              methodList.add(e);
            }
          }
          return null;
        }

        @Override
        public Void visitPackage(PackageElement e, Void unused) {
          return null;
        }

        @Override
        public Void visitVariable(VariableElement e, Void unused) {
          return null;
        }

        @Override
        public Void visitTypeParameter(TypeParameterElement e, Void unused) {
          return null;
        }
      };
}
