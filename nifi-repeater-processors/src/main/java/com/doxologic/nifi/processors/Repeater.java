/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.doxologic.nifi.processors;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@Tags({"routing"})
@CapabilityDescription("Restrict the number of repeated passages FlowFile. Number of passes are defined by property.")
//@SeeAlso({})
//@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="repeater.count", description="Number of current passes.")})
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class Repeater extends AbstractProcessor {

    private static final PropertyDescriptor REPEAT_COUNT = new PropertyDescriptor
            .Builder().name("Repeat Count")
            .description("Number of cycles for incoming FlowFile.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final PropertyDescriptor PENALIZE_PASSES = new PropertyDescriptor
            .Builder().name("Penalize Repeated Passes")
            .description("Penalize FlowFile after passes this processor more than once.")
            .required(true)
            .defaultValue("false")
            .allowableValues(new AllowableValue("true"), new AllowableValue("false"))
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final Relationship REPEAT = new Relationship.Builder()
            .name("repeat")
            .description("FlowFiles that can be repeated.")
            .build();

    private static final Relationship NO_REPEAT = new Relationship.Builder()
            .name("no-repeat")
            .description("FlowFiles that cannot be repeated.")
            .build();
    private static final String REPEATER_COUNT_ATTR = "repeater.count";

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(REPEAT_COUNT);
        descriptors.add(PENALIZE_PASSES);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REPEAT);
        relationships.add(NO_REPEAT);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        int maxRepeatCount = context.getProperty(REPEAT_COUNT).asInteger();
        boolean penalizePasses = context.getProperty(PENALIZE_PASSES).asBoolean();
        int repeatCount = NumberUtils.toInt(flowFile.getAttribute(REPEATER_COUNT_ATTR), 0) + 1;
        HashMap<String, String> attributes = new HashMap<>();

        attributes.put(REPEATER_COUNT_ATTR, String.valueOf(repeatCount));
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().modifyAttributes(flowFile);

        if (penalizePasses && repeatCount > 1) {
            flowFile = session.penalize(flowFile);
        }

        if (repeatCount <= maxRepeatCount) { // route to repeat relationship
            session.transfer(flowFile, REPEAT);
        } else { // route to no-repeat relationship
            session.transfer(flowFile, NO_REPEAT);
        }
    }
}
