open Riak
open Sys
open Lwt
open Lwt_unix

let test_ping conn =
  match_lwt riak_ping conn with
    | true ->
        print_endline "Pong";
        return ()
    | false ->
        return ()

let _ =
  run (
    let pbip = 10017 in
    try_lwt
      lwt conn = riak_connect_with_defaults "127.0.0.1" pbip in
      lwt _result = test_ping conn in
      riak_disconnect conn;
    with Unix.Unix_error (e, _, _) ->
      print_endline "Pang";
      return ()
  )
