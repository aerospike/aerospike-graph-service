package com.aerospike.firefly.process.call.sindex;

import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.aerospike.admin.AdminService;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.Map;

public abstract class SindexServiceBase<I, R> extends AdminService<I, R> {
    protected static final String ELEMENT_TYPE = "element_type";
    protected static final String PROPERTY_KEY = "property_key";
    protected static final String INDEX_TYPE = "index_type";
    protected static final Map<String, IndexType> INDEX_TYPE_LOOKUP = new HashMap<>();
    static {
        INDEX_TYPE_LOOKUP.put("string", IndexType.STRING);
        INDEX_TYPE_LOOKUP.put("numeric", IndexType.NUMERIC);
    }

    public SindexServiceBase(final FireflyGraph graph) {
        super(graph);
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }

    @Override
    protected String getAdminNamespace() {
        return "index";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }
}
