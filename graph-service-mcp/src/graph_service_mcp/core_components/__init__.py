def register_all(mcp, g, client):
    from .gremlin import register as reg_gremlin
    from .prompts import register as reg_prompts
    from .resources import register as reg_resources
    reg_gremlin(mcp, g, client)
    reg_prompts(mcp, g)
    reg_resources(mcp, g)
