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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;


public class RepeaterTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(Repeater.class);
    }

    @Test
    public void testProcessor() {
        MockFlowFile flowFile = new MockFlowFile(12);

        testRunner.enqueue(flowFile);
        testRunner.setProperty("Repeat Count", "1");
        testRunner.run();

        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred("repeat", 1);
        testRunner.assertValid();

        flowFile = testRunner.getFlowFilesForRelationship("repeat").get(0);
        Assert.assertEquals("1", flowFile.getAttribute("repeater.count"));

        testRunner.enqueue(flowFile);
        testRunner.clearTransferState();
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred("no-repeat", 1);
        testRunner.assertQueueEmpty();

        flowFile = testRunner.getFlowFilesForRelationship("no-repeat").get(0);
        Assert.assertEquals("2", flowFile.getAttribute("repeater.count"));
        Assert.assertFalse(flowFile.isPenalized());
    }

    @Test
    public void penalizedTest() {
        MockFlowFile flowFile = new MockFlowFile(12);

        flowFile.putAttributes(new HashMap<String, String>() {{
            put("repeater.count", "1");
        }});
        testRunner.enqueue(flowFile);
        testRunner.setProperty("Repeat Count", "1");
        testRunner.setProperty("Penalize Repeated Passes", "true");
        testRunner.run();

        testRunner.assertQueueEmpty();
        testRunner.assertAllFlowFilesTransferred("no-repeat", 1);
        testRunner.assertValid();

        flowFile = testRunner.getFlowFilesForRelationship("no-repeat").get(0);
        Assert.assertEquals("2", flowFile.getAttribute("repeater.count"));
        Assert.assertTrue(flowFile.isPenalized());
    }

}
