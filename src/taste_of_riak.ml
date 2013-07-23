open Riak
open Sys
open Lwt
open Lwt_unix

let my_ping conn =
  print_endline "Ping";
  match_lwt riak_ping conn with
    | true ->
        print_endline "\tPong";
        return ()
    | false ->
        return ()

let put_some_data conn bucket key value =
  let msg = "Put: bucket=" ^ bucket ^ ", key = " ^ key ^ ", value = " ^ value in
  print_endline msg;
  lwt _result = riak_put conn bucket
                              (Some key)
                              value
                              [] in
  return ()

let get_some_data conn bucket key =
  let msg = "Get: bucket=" ^ bucket ^ ", key = " ^ key in
    print_endline msg;
  lwt obj = riak_get conn bucket key [] in
  match obj with
      | Some o ->
          (match o.obj_value with
              | Some v -> print_endline ("\tValue = " ^ v);
                          return ()
              | None -> print_endline "\tNo value";
                        return ())
      | None -> print_endline "\tNot found";
                return ()

let delete_some_data conn bucket key =
  let msg = "Delete: bucket=" ^ bucket ^ ", key = " ^ key in
    print_endline msg;
  lwt _ = riak_del conn bucket key [] in
    return ()

let _ =
  run (
    let pbip = 10017 in
    let my_bucket = "MyBucket" in
    let my_key    = "MyKey" in
    let my_value  = "MyValue" in
    try_lwt
      lwt conn = riak_connect_with_defaults "127.0.0.1" pbip in
      lwt _result = my_ping conn in
      lwt _ = put_some_data conn my_bucket my_key my_value in
      lwt _ = get_some_data conn my_bucket my_key in
      lwt _ = delete_some_data conn my_bucket my_key in
      lwt _ = get_some_data conn my_bucket my_key in
      riak_disconnect conn;
    with Unix.Unix_error (e, _, _) ->
      print_endline "Pang";
      return ()
  )
