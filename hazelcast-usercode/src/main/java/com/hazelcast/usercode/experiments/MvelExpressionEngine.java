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

package com.hazelcast.usercode.experiments;

// see:
// https://java-source.net/open-source/expression-languages
// https://github.com/oldratlee/java-modern-tech-practice/issues/11

// engines:
// JEXL https://commons.apache.org/proper/commons-jexl/
// JUEL https://github.com/beckchr/juel/
// MVEL https://github.com/mvel/mvel
// EVALEX https://github.com/ezylang/EvalEx

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import org.mvel2.MVEL;
import org.mvel2.ParserContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class MvelExpressionEngine implements ExpressionEngine {

    private final ObjectFactory objectFactory = new ObjectFactory();

    // TODO
    //  this is totally experimental
    //  we may want to compile the expression for reuse

    // TODO should be a cache with LRU eviction and max count, LinkedList-based
    private final Map<String, CompiledExpression> compiledExpressions = new HashMap<>();
    private final LinkedList<String> usageList = new LinkedList<>();
    private final int cacheSize = 100;

    private final ParserContext parserContext = new ParserContext();
    //parserContext.addImport(CalcHelper.class.getSimpleName(), CalcHelper.class);

    private class CompiledExpression {
        public Serializable expr;
        public String[] varNames;
    }

    private CompiledExpression getExpression(String expr) {
        boolean removed = usageList.remove(expr);
        if (removed) {
            usageList.addFirst(expr);
            return compiledExpressions.get(expr);
        }
        return null;
    }

    private void setExpression(String expr, CompiledExpression compiledExpression) {
        if (usageList.size() == cacheSize) {
            String removeExpr = usageList.removeLast();
            compiledExpressions.remove(removeExpr);
        }
        usageList.addFirst(expr);
        compiledExpressions.put(expr, compiledExpression);
    }

    @Override
    public Object eval(String expr, Object... input) {

        CompiledExpression compiledExpression = getExpression(expr);

        if (compiledExpression == null) {
            int pos = expr.indexOf("->");
            String[] varNames = expr.substring(0, pos).split(",");
            if (varNames.length != input.length) {
                throw new IllegalArgumentException();
            }
            String exprBody = expr.substring(pos + 2).trim();
            compiledExpression = new CompiledExpression();
            compiledExpression.expr = MVEL.compileExpression(exprBody, parserContext);
            compiledExpression.varNames = varNames;
            setExpression(expr, compiledExpression);
        }

        Map<String, Object> vars = new HashMap<>();
        vars.put("f", objectFactory); // FIXME better reserved name?
        for (int i = 0; i < compiledExpression.varNames.length; i++)
            vars.put(compiledExpression.varNames[i].trim(), input[i]);
        //return MVEL.eval(exprBody, vars);
        return MVEL.executeExpression(compiledExpression.expr, parserContext, vars);
    }

    public final class ObjectFactory {

        public <T> T Coalesce(T value, T defaultValue) {
            return value == null ? defaultValue : value;
        }

        public JsonObject JsonObject(HazelcastJsonValue jsonValue) {
            return new JsonObject(Json.parse(jsonValue.toString()).asObject());
        }

        public JsonObject JsonObject(String jsonString) {
            return new JsonObject(Json.parse(jsonString).asObject());
        }

        public <E0, E1> Tuple2<E0, E1> Tuple(E0 e0, E1 e1) {
            return Tuple2.tuple2(e0, e1);
        }

        public <E0, E1, E2> Tuple3<E0, E1, E2> Tuple(E0 e0, E1 e1, E2 e2) {
            return Tuple3.tuple3(e0, e1, e2);
        }
    }
}
