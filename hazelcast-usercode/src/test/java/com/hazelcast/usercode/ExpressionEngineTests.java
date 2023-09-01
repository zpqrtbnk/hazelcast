/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.usercode;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.usercode.experiments.ExpressionEngine;
import com.hazelcast.usercode.experiments.MvelExpressionEngine;
import org.junit.Test;
import org.mvel2.MVEL;

import java.util.HashMap;
import java.util.Map;

public class ExpressionEngineTests {

    @Test
    public void Test1() {
        String expression = "2 + foo";
        Map<String, Object> vars = new HashMap<>();
        vars.put("foo", 33);
        Object resultObject = MVEL.eval(expression, vars);
        int result = (int) resultObject;
        System.out.println(result);
    }

    @Test
    public void Test2() {
        //String expression = "[ \"key\":x.key, \"value\":x.value+3 ]";
        String expression = "ctx.NewMapEntry(x.key, x.value+3)";
        Map<String, Object> vars = new HashMap<>();
        vars.put("ctx", this);
        vars.put("x", new MapEntry<>("key", 33));
        Object resultObject = MVEL.eval(expression, vars);
        System.out.println(resultObject.getClass());
        if (resultObject instanceof HashMap) {
            System.out.println("Map:");
            HashMap map = (HashMap)resultObject;
            for (Object key : map.keySet()) {
                System.out.println("  " + key + ":" + map.get(key));
            }
        }
        else {
            System.out.println(resultObject.getClass());
            System.out.println(resultObject);
        }
        // FIXME how would I map it back to a MapEntry?
        // in other words HOW can I, after an expression, cast the result to a given type?
    }

    @Test
    public void Test3() {
        ExpressionEngine ee = new MvelExpressionEngine();
        Object result = ee.eval("x -> f.Tuple(x.key, x.value)", new MapEntry<>("key", 33));
        Tuple2 tuple = (Tuple2) result;
        System.out.println(tuple.f0());
        System.out.println(tuple.f1());
    }

    @Test
    public void Test4() {
        ExpressionEngine ee = new MvelExpressionEngine();
        Object result = ee.eval("x ->f.Coalesce(x.key, \"KEY\") + \" \" + f.Coalesce(x.value, \"VALUE\")",
                new MapEntry<String, String>("key", null));
        System.out.println(result);
    }

    @Test
    public void Test5() {
        ExpressionEngine ee = new MvelExpressionEngine();

        // FIXME one expression engine is having issues with overloads
        // this works if we use 2 different f names JsonObject JsonObject2
        // but with one name there's a type conversion issue on the second one?
        // -> force toString() on the HazelcastJsonValue, cannot rely on overload

        HazelcastJsonValue jsonValue = new HazelcastJsonValue("{ \"meh\": 33 }");
        Object result = ee.eval("x -> f.JsonObject(x.toString())", jsonValue);
        System.out.println(result);

        String jsonString = "{ \"meh\": 33 }";
        Object result2 = ee.eval("x -> f.JsonObject(x)", jsonString);
        System.out.println(result2);

        Object result3 = ee.eval("x -> f.JsonObject(x).getInt(\"meh\", -1)", jsonString);
        System.out.println(result3);
    }

    @Test
    public void Test6() {
        ExpressionEngine ee = new MvelExpressionEngine();
        System.out.println(ee.eval("x->f.Tuple(2,3)", ""));
        System.out.println(ee.eval("x -> f.Tuple(x,\"duh\")", ""));
    }

    @Test
    public void Test7() {
        ExpressionEngine ee = new MvelExpressionEngine();
        System.out.println(ee.eval("x,y->x+y", 2,3));
    }

    @Test
    public void Test8() {
        ExpressionEngine ee = new MvelExpressionEngine();

        // integer value 2 ?!
        Object result = ee.eval("x,y->(x,y)", 2,"meh");
        System.out.println(result.getClass());
        System.out.println(result);

        // java.util.ArrayList w/ two values
        result = ee.eval("x,y->[x,y]", 2,"meh");
        System.out.println(result.getClass());
        System.out.println(result);
    }

    public MapEntry NewMapEntry(Object key, Object value) {
        return new MapEntry(key, value);
    }

    public class  MapEntry <TK,TV> {
        public MapEntry(TK key, TV value) {
            this.key = key;
            this.value = value;
        }
        private TK key;
        public TK getKey() { return key; }
        private TV value;
        public TV getValue() { return value; }

        @Override
        public String toString() {
            return "MapEntry(key=" + key + ", value=" + value + ")";
        }
    }
}
