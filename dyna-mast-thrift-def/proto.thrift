namespace java com.dynamic.mastering //

include "dynamic_mastering.thrift"

struct att_desc
{
    1:string    name,
    2:i32       tableid,
    3:i32       columnid,
    4:i32       format,
    5:i32       typid,
    6:i32       typlen,
    7:i32       atttypmod
}

struct query_result {
    1:i32 ntups,
    2:i32 numAttributes,
    3:i32 tupArrSize,
    4:string errMsg,
    5:list<att_desc> attDescs,
    6:list<list<string>> tuples,
    7:i32 numAffectedRows,
    8:dynamic_mastering.exec_status_type status
}

struct site_selector_begin_result {
    1:dynamic_mastering.exec_status_type status,
    2:string ip;
    3:i32 port;
    4:list<dynamic_mastering.primary_key> items_at_site;
}

struct begin_result {
    1:dynamic_mastering.exec_status_type status,
    2:string errMsg
}

struct abort_result {
    1:dynamic_mastering.exec_status_type status,
    2:string errMsg
}

struct commit_result {
    1:dynamic_mastering.exec_status_type status,
    2:dynamic_mastering.site_version_vector session_version_vector,
    3:string errMsg
}

struct grant_result {
    1:dynamic_mastering.exec_status_type status,
    2:dynamic_mastering.site_version_vector session_version_vector,
}

struct release_result {
    1:dynamic_mastering.exec_status_type status,
    2:dynamic_mastering.site_version_vector session_version_vector,
}

struct site_statistics_results {
    1:dynamic_mastering.exec_status_type status,
    2:dynamic_mastering.site_version_vector site_version_vector,
    // future: whatever else we need to do site selector strategies
}

struct sproc_result {
    1:dynamic_mastering.exec_status_type status,
    2:i32 return_code,
}

service site_manager {
    begin_result rpc_begin_transaction(1:dynamic_mastering.clientid id,
                                       2:dynamic_mastering.site_version_vector client_session_version_vector,
                                       3:list<dynamic_mastering.bucket_key> write_set),

    query_result rpc_execute(1:dynamic_mastering.clientid id, 2:string query),
    query_result rpc_select(1:dynamic_mastering.clientid id, 2:string query),
    query_result rpc_insert(1:dynamic_mastering.clientid id, 2:string query),
    query_result rpc_delete(1:dynamic_mastering.clientid id, 2:string query),
    query_result rpc_update(1:dynamic_mastering.clientid id, 2:string query),
    sproc_result rpc_stored_procedure(1:dynamic_mastering.clientid id, 2:string name, 3:binary sproc_args),
    commit_result rpc_prepare_transaction(1:dynamic_mastering.clientid id),
    commit_result rpc_commit_transaction(1:dynamic_mastering.clientid id),
    abort_result rpc_abort_transaction(1:dynamic_mastering.clientid id),

    grant_result rpc_grant_mastership(1:dynamic_mastering.clientid id,
                                      2:list<dynamic_mastering.bucket_key> keys,
                                      3:dynamic_mastering.site_version_vector session_version_vector),
    release_result rpc_release_mastership(1:dynamic_mastering.clientid id,
                                          2:list<dynamic_mastering.bucket_key> keys,
                                          3:dynamic_mastering.site_version_vector session_version_vector),

    site_statistics_results rpc_get_site_statistics(),

}

service SiteSelector {
    list<site_selector_begin_result>
    rpc_begin_transaction(1:dynamic_mastering.clientid id,
                          2:dynamic_mastering.site_version_vector client_session_version_vector,
                          3:list<dynamic_mastering.primary_key> write_set),
}
