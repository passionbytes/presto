/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spark;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.eventlistener.EventListenerModule;
import com.facebook.presto.security.AccessControlModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.spark_project.guava.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.server.PrestoSystemRequirements.verifyJvmRequirements;
import static com.facebook.presto.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;
import static java.util.Objects.requireNonNull;

public class PrestoSparkInjectorFactory
{
    private final Map<String, String> properties;
    private final List<Module> additionalModules;

    public PrestoSparkInjectorFactory(Map<String, String> properties, List<Module> additionalModules)
    {
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.additionalModules = ImmutableList.copyOf(requireNonNull(additionalModules, "additionalModules is null"));
    }

    public Injector create()
    {
        verifyJvmRequirements();
        verifySystemTimeIsReasonable();

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new AccessControlModule(),
                new JsonModule(),
                new EventListenerModule(),
                new PrestoSparkModule());

        modules.addAll(additionalModules);

        Bootstrap app = new Bootstrap(modules.build());
        app.setRequiredConfigurationProperties(properties);

        return app.strictConfig().initialize();
    }
}
