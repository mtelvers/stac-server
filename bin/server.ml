open Stac_lib

let setup_logging () =
  Fmt_tty.setup_std_outputs ();
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level (Some Logs.Info)

let get_param uri name = Uri.get_query_param uri name

let get_int_param uri name ~default =
  get_param uri name |> Option.map int_of_string_opt |> Option.join
  |> Option.value ~default

let cors_headers content_type =
  Cohttp.Header.of_list
    [
      ("Content-Type", content_type);
      ("Access-Control-Allow-Origin", "*");
      ("Access-Control-Allow-Methods", "GET, OPTIONS");
      ("Access-Control-Allow-Headers", "*");
    ]

let respond ~status ?(content_type = "application/json") body =
  Cohttp_eio.Server.respond_string ~status ~headers:(cors_headers content_type) ~body ()

let respond_json json =
  respond ~status:`OK (Yojson.Safe.to_string json)

let respond_geojson json =
  respond ~status:`OK ~content_type:"application/geo+json" (Yojson.Safe.to_string json)

let respond_not_found msg =
  respond ~status:`Not_found
    (Yojson.Safe.to_string (`Assoc [ ("code", `String "NotFound"); ("description", `String msg) ]))

let respond_options () = respond ~status:`OK ""

let respond_method_not_allowed () =
  respond ~status:`Method_not_allowed "Method not allowed"

let split_path path =
  String.split_on_char '/' path |> List.filter (fun s -> s <> "")

let handle_landing ~base_url =
  Stac_json.landing_page ~base_url |> respond_json

let handle_conformance () = Stac_json.conformance |> respond_json

let tile_count_for_year reg year =
  Array.to_seq reg.Registry.tiles
  |> Seq.filter (fun (t : Registry.tile) -> t.year = year)
  |> Seq.fold_left (fun acc _ -> acc + 1) 0

let parse_bbox uri =
  match get_param uri "bbox" with
  | Some bbox_str -> (
      match String.split_on_char ',' bbox_str |> List.filter_map float_of_string_opt with
      | [ a; b; c; d ] -> (a, b, c, d)
      | _ -> (-180., -90., 180., 90.))
  | None -> (-180., -90., 180., 90.)

let year_of_collection_id collection_id =
  match String.split_on_char '-' collection_id with
  | _ :: year_s :: _ -> int_of_string_opt year_s
  | _ -> None

let search_tiles ~base_url reg store_configs ~year ~minx ~miny ~maxx ~maxy ~limit ~offset =
  let matching_tiles =
    Registry.tiles_in_bbox reg ~year ~minx ~miny ~maxx ~maxy |> Array.of_seq
  in
  let matched = Array.length matching_tiles in
  let page = Array.sub matching_tiles (min offset matched) (min limit (max 0 (matched - offset))) in
  let items =
    Array.to_list page
    |> List.map (fun tile ->
           let stores = Registry.stores_for_tile reg tile.Registry.id in
           Stac_json.item ~base_url ~tile ~stores ~store_configs)
  in
  let next_token =
    if offset + limit < matched then Some (string_of_int (offset + limit)) else None
  in
  Stac_json.item_collection ~items ~matched ~returned:(List.length items) ~next_token
  |> respond_geojson

let handle_bitmap reg uri =
  let year = get_int_param uri "year" ~default:0 in
  let width = 3600 and height = 1800 in
  let row_bytes = (width + 7) / 8 in
  let buf = Bytes.make (row_bytes * height) '\000' in
  Array.iter
    (fun (t : Registry.tile) ->
      if year = 0 || t.year = year then (
        let x = int_of_float (Float.round ((t.lon -. 0.05 +. 180.0) /. 0.1)) in
        let y = int_of_float (Float.round ((90.0 -. (t.lat -. 0.05) -. 0.1) /. 0.1)) in
        if x >= 0 && x < width && y >= 0 && y < height then (
          let byte_idx = y * row_bytes + (x / 8) in
          let bit = 1 lsl (7 - (x land 7)) in
          let old = Char.code (Bytes.get buf byte_idx) in
          Bytes.set buf byte_idx (Char.chr (old lor bit)))))
    reg.Registry.tiles;
  let headers =
    Cohttp.Header.of_list [
      ("Content-Type", "application/octet-stream");
      ("Access-Control-Allow-Origin", "*");
      ("Access-Control-Expose-Headers", "X-Width,X-Height");
      ("X-Width", string_of_int width);
      ("X-Height", string_of_int height);
      ("Cache-Control", "public, max-age=300");
    ]
  in
  Cohttp_eio.Server.respond_string ~status:`OK ~headers
    ~body:(Bytes.to_string buf) ()

let handle_coverage reg store_configs _uri =
  let store_names = List.map fst store_configs in
  let num_stores = List.length store_names in
  let store_tables =
    List.map
      (fun name ->
        Hashtbl.find_opt reg.Registry.stores name
        |> Option.value ~default:(Hashtbl.create 0))
      store_names
  in
  let cells : (int * int, int array) Hashtbl.t =
    Hashtbl.create (1 lsl 20)
  in
  Array.iter
    (fun (t : Registry.tile) ->
      let lon_k = int_of_float (Float.round (t.lon *. 1_000_000.)) in
      let lat_k = int_of_float (Float.round (t.lat *. 1_000_000.)) in
      let key = (lon_k, lat_k) in
      let counts =
        match Hashtbl.find_opt cells key with
        | Some c -> c
        | None ->
            let c = Array.make (num_stores + 1) 0 in
            Hashtbl.replace cells key c; c
      in
      counts.(0) <- counts.(0) + 1;
      List.iteri
        (fun i tbl -> if Hashtbl.mem tbl t.id then counts.(i + 1) <- counts.(i + 1) + 1)
        store_tables)
    reg.Registry.tiles;
  let buf = Buffer.create (1 lsl 16) in
  let total_cells, anomaly_cells =
    Hashtbl.fold
      (fun (lon_k, lat_k) counts (total, anomalies) ->
        let tile_total = counts.(0) in
        let fully_replicated =
          let rec count i acc =
            if i > num_stores then acc
            else count (i + 1) (if counts.(i) = tile_total then acc + 1 else acc)
          in
          count 1 0
        in
        if fully_replicated < num_stores then (
          Buffer.add_int32_le buf (Int32.of_int (lon_k + 50_000));
          Buffer.add_int32_le buf (Int32.of_int (lat_k + 50_000));
          Buffer.add_char buf (Char.chr fully_replicated);
          (total + 1, anomalies + 1))
        else (total + 1, anomalies))
      cells (0, 0)
  in
  let headers =
    Cohttp.Header.of_list
      [
        ("Content-Type", "application/octet-stream");
        ("Access-Control-Allow-Origin", "*");
        ("Access-Control-Expose-Headers",
         "X-Total-Stores,X-Cell-Size-Deg,X-Total-Cells,X-Anomaly-Cells");
        ("X-Total-Stores", string_of_int num_stores);
        ("X-Cell-Size-Deg", "0.1");
        ("X-Total-Cells", string_of_int total_cells);
        ("X-Anomaly-Cells", string_of_int anomaly_cells);
        ("Cache-Control", "public, max-age=60");
      ]
  in
  Cohttp_eio.Server.respond_string ~status:`OK ~headers
    ~body:(Buffer.contents buf) ()

let handle_collections ~base_url reg store_configs =
  let store_names = List.map fst store_configs in
  let tile_counts = List.map (tile_count_for_year reg) reg.Registry.years in
  Stac_json.collections_response ~base_url ~years:reg.years ~tile_counts ~store_names |> respond_json

let handle_collection ~base_url reg store_configs collection_id =
  match year_of_collection_id collection_id with
  | Some year when List.mem year reg.Registry.years ->
      let store_names = List.map fst store_configs in
      Stac_json.collection ~base_url ~year ~tile_count:(tile_count_for_year reg year) ~store_names
      |> respond_json
  | _ -> respond_not_found ("Collection not found: " ^ collection_id)

let handle_items ~base_url reg store_configs uri collection_id =
  match year_of_collection_id collection_id with
  | Some year when List.mem year reg.Registry.years ->
      let minx, miny, maxx, maxy = parse_bbox uri in
      let limit = get_int_param uri "limit" ~default:1000 in
      let offset = get_int_param uri "offset" ~default:0 in
      search_tiles ~base_url reg store_configs ~year ~minx ~miny ~maxx ~maxy ~limit ~offset
  | _ -> respond_not_found ("Collection not found: " ^ collection_id)

let handle_item ~base_url reg store_configs collection_id item_id =
  match Registry.tile_by_id reg item_id with
  | None -> respond_not_found ("Item not found: " ^ item_id)
  | Some tile ->
      let expected_collection = "geotessera-" ^ string_of_int tile.year in
      if collection_id <> expected_collection then
        respond_not_found ("Item not found in collection: " ^ collection_id)
      else
        let stores = Registry.stores_for_tile reg tile.id in
        Stac_json.item ~base_url ~tile ~stores ~store_configs |> respond_geojson

let handle_search ~base_url reg store_configs uri =
  let minx, miny, maxx, maxy = parse_bbox uri in
  let limit = get_int_param uri "limit" ~default:1000 in
  let offset = get_int_param uri "offset" ~default:0 in
  let year =
    match get_param uri "datetime" with
    | Some dt ->
        String.split_on_char '-' dt
        |> List.hd |> int_of_string_opt
        |> Option.value ~default:2024
    | None -> get_int_param uri "year" ~default:2024
  in
  let year_matches =
    match get_param uri "collections" with
    | None | Some "" -> true
    | Some c ->
        String.split_on_char ',' c
        |> List.exists (fun cid -> year_of_collection_id cid = Some year)
  in
  if not year_matches then
    Stac_json.item_collection ~items:[] ~matched:0 ~returned:0 ~next_token:None
    |> respond_geojson
  else
    search_tiles ~base_url reg store_configs ~year ~minx ~miny ~maxx ~maxy ~limit ~offset

let route ~base_url reg store_configs uri =
  let path = Uri.path uri in
  match split_path path with
  | [] -> handle_landing ~base_url
  | [ "conformance" ] -> handle_conformance ()
  | [ "collections" ] -> handle_collections ~base_url reg store_configs
  | [ "collections"; collection_id ] -> handle_collection ~base_url reg store_configs collection_id
  | [ "collections"; collection_id; "items" ] ->
      handle_items ~base_url reg store_configs uri collection_id
  | [ "collections"; collection_id; "items"; item_id ] ->
      handle_item ~base_url reg store_configs collection_id item_id
  | [ "search" ] -> handle_search ~base_url reg store_configs uri
  | [ "coverage" ] -> handle_coverage reg store_configs uri
  | [ "bitmap" ] -> handle_bitmap reg uri
  | _ -> respond_not_found ("Unknown path: " ^ path)

let make_callback ~base_url reg_ref store_configs _conn req _body =
  let uri = Cohttp.Request.uri req in
  let meth = Cohttp.Request.meth req in
  Logs.debug (fun m -> m "%s %s" (Cohttp.Code.string_of_method meth) (Uri.path uri));
  match meth with
  | `GET -> route ~base_url !reg_ref store_configs uri
  | `OPTIONS -> respond_options ()
  | _ -> respond_method_not_allowed ()

let parse_store_configs () =
  match Sys.getenv_opt "STORES" with
  | None | Some "" -> []
  | Some stores_str ->
      String.split_on_char ';' stores_str
      |> List.filter_map (fun entry ->
             match String.split_on_char '=' entry with
             | name :: rest ->
                 let url = String.concat "=" rest in
                 Some (name, Registry.{ name; base_url = url; path_pattern = "" })
             | _ -> None)

let () =
  setup_logging ();
  let data_dir =
    Sys.getenv_opt "DATA_DIR" |> Option.value ~default:"./data"
  in
  let port =
    Sys.getenv_opt "PORT" |> Option.map int_of_string_opt |> Option.join
    |> Option.value ~default:8000
  in
  let base_url =
    Sys.getenv_opt "BASE_URL" |> Option.value ~default:("http://localhost:" ^ string_of_int port)
  in

  let store_configs = parse_store_configs () in
  if store_configs = [] then (
    Logs.err (fun m -> m "STORES env var is required (e.g. STORES=okavango=url;s3=url)");
    exit 1);

  let stores =
    List.map (fun (name, _cfg) -> (name, data_dir ^ "/" ^ name ^ ".parquet")) store_configs
  in
  let reg_ref = ref (Registry.create ~stores) in

  Logs.app (fun m -> m "Starting STAC server on port %d" port);
  Logs.app (fun m -> m "Base URL: %s" base_url);
  Logs.app (fun m -> m "Stores: %s" (Registry.store_names !reg_ref |> String.concat ", "));

  let check_interval =
    Sys.getenv_opt "RELOAD_INTERVAL" |> Option.map float_of_string_opt
    |> Option.join |> Option.value ~default:10.0
  in

  Eio_main.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let fs = Eio.Stdenv.fs env in

  let get_mtimes () =
    List.filter_map
      (fun (_, path) ->
        try
          let stat = Eio.Path.stat ~follow:true Eio.Path.(fs / path) in
          Some (path, stat.mtime)
        with Eio.Io _ -> None)
      stores
  in

  let server =
    Cohttp_eio.Server.make
      ~callback:(make_callback ~base_url reg_ref store_configs) ()
  in

  let watch_files () =
    let last_mtimes = ref (get_mtimes ()) in
    while true do
      Eio.Time.sleep clock check_interval;
      let current_mtimes = get_mtimes () in
      if current_mtimes <> !last_mtimes then (
        Logs.app (fun m -> m "Parquet files changed, reloading...");
        (try
          reg_ref := Registry.create ~stores;
          Logs.app (fun m -> m "Reload complete: %s"
            (Registry.store_names !reg_ref |> String.concat ", "))
        with exn ->
          Logs.err (fun m -> m "Reload failed: %s (keeping old data)" (Printexc.to_string exn)));
        last_mtimes := current_mtimes)
    done
  in

  Eio.Switch.run @@ fun sw ->
  let socket =
    Eio.Net.listen net ~sw ~backlog:128 ~reuse_addr:true
      (`Tcp (Eio.Net.Ipaddr.V4.any, port))
  in
  Eio.Fiber.both
    (fun () ->
      Cohttp_eio.Server.run socket server
        ~on_error:(fun exn -> Logs.err (fun m -> m "Server error: %s" (Printexc.to_string exn))))
    watch_files
