from . import server

def create_mcp_server(run_server: bool = True):
    return server.start_mcp_server(run_server=run_server)