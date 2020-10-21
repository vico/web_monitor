
function dftGetURI(useProtocol)
{
    var protocol = useProtocol ? "http://" : "/";
    var host = location.hostname;
    var port = location.port;
    // pathname = /ad/xxxx/xxxxx/xxxxx
    var params = location.pathname.split("/");
    var webroot = params[1];
    var ret = "";
    if ( useProtocol ) {
        ret = protocol + host + (port != 80 ? ':' + port : '') + "/" + webroot;
    }
    else {
        ret = "/" + webroot;
    }
    return ret;
}
