open Stac_lib

let setup_logging () =
  Fmt_tty.setup_std_outputs ();
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level (Some Logs.Info)

let get_param uri name = Uri.get_query_param uri name

let get_float_param uri name ~default =
  get_param uri name |> Option.map float_of_string_opt |> Option.join
  |> Option.value ~default

let get_int_param uri name ~default =
  get_param uri name |> Option.map int_of_string_opt |> Option.join
  |> Option.value ~default

let json_headers =
  Cohttp.Header.of_list
    [
      ("Content-Type", "application/json");
      ("Access-Control-Allow-Origin", "*");
      ("Access-Control-Allow-Methods", "GET, OPTIONS");
      ("Access-Control-Allow-Headers", "*");
    ]

let geojson_headers =
  Cohttp.Header.of_list
    [
      ("Content-Type", "application/geo+json");
      ("Access-Control-Allow-Origin", "*");
      ("Access-Control-Allow-Methods", "GET, OPTIONS");
      ("Access-Control-Allow-Headers", "*");
    ]

let respond_json json =
  let body = Yojson.Safe.to_string json in
  Cohttp_lwt_unix.Server.respond_string ~status:`OK ~headers:json_headers ~body ()

let respond_geojson json =
  let body = Yojson.Safe.to_string json in
  Cohttp_lwt_unix.Server.respond_string ~status:`OK ~headers:geojson_headers ~body ()

let respond_not_found msg =
  let body = Yojson.Safe.to_string (`Assoc [ ("code", `String "NotFound"); ("description", `String msg) ]) in
  Cohttp_lwt_unix.Server.respond_string ~status:`Not_found ~headers:json_headers ~body ()

let respond_options () =
  Cohttp_lwt_unix.Server.respond_string ~status:`OK ~headers:json_headers ~body:"" ()

let respond_method_not_allowed () =
  Cohttp_lwt_unix.Server.respond_string ~status:`Method_not_allowed ~body:"Method not allowed" ()

(* Parse path segments: "/collections/geotessera-2024/items/grid_1.0_2.0" *)
let split_path path =
  String.split_on_char '/' path |> List.filter (fun s -> s <> "")

let handle_landing ~base_url =
  Stac_json.landing_page ~base_url |> respond_json

let handle_conformance () = Stac_json.conformance |> respond_json

let handle_coverage reg store_configs _uri =
  (* Aggregate across all years by the tile's exact SW corner (lon,lat). Emits
     one 9-byte record per cell where some store is missing the tile in at
     least one year: i32 lon_mdeg LE, i32 lat_mdeg LE, u8 fully_replicated_count.
     Coordinates point at the cell centre (SW + 0.05°). *)
  let store_names = List.map fst store_configs in
  let num_stores = List.length store_names in
  let store_tables =
    List.map
      (fun name ->
        Hashtbl.find_opt reg.Registry.stores name
        |> Option.value ~default:(Hashtbl.create 0))
      store_names
  in
  (* Key each cell by SW corner expressed as microdegrees to avoid float hashing. *)
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
  let total_cells = ref 0 in
  let anomaly_cells = ref 0 in
  Hashtbl.iter
    (fun (lon_k, lat_k) counts ->
      incr total_cells;
      let total = counts.(0) in
      let fully = ref 0 in
      for i = 1 to num_stores do
        if counts.(i) = total then incr fully
      done;
      if !fully < num_stores then begin
        incr anomaly_cells;
        let center_lon_mdeg = lon_k + 50_000 in
        let center_lat_mdeg = lat_k + 50_000 in
        Buffer.add_int32_le buf (Int32.of_int center_lon_mdeg);
        Buffer.add_int32_le buf (Int32.of_int center_lat_mdeg);
        Buffer.add_char buf (Char.chr !fully)
      end)
    cells;
  let headers =
    Cohttp.Header.of_list
      [
        ("Content-Type", "application/octet-stream");
        ("Access-Control-Allow-Origin", "*");
        ("Access-Control-Expose-Headers",
         "X-Total-Stores,X-Cell-Size-Deg,X-Total-Cells,X-Anomaly-Cells");
        ("X-Total-Stores", string_of_int num_stores);
        ("X-Cell-Size-Deg", "0.1");
        ("X-Total-Cells", string_of_int !total_cells);
        ("X-Anomaly-Cells", string_of_int !anomaly_cells);
        ("Cache-Control", "public, max-age=60");
      ]
  in
  Cohttp_lwt_unix.Server.respond_string ~status:`OK ~headers
    ~body:(Buffer.contents buf) ()

let handle_collections ~base_url reg store_configs =
  let years = reg.Registry.years in
  let store_names = List.map fst store_configs in
  let tile_counts =
    List.map
      (fun year ->
        Array.to_seq reg.Registry.tiles
        |> Seq.filter (fun (t : Registry.tile) -> t.year = year)
        |> Seq.fold_left (fun acc _ -> acc + 1) 0)
      years
  in
  Stac_json.collections_response ~base_url ~years ~tile_counts ~store_names |> respond_json

let handle_collection ~base_url reg store_configs collection_id =
  (* collection_id is like "geotessera-2024" *)
  match String.split_on_char '-' collection_id with
  | _ :: year_s :: _ -> (
      match int_of_string_opt year_s with
      | Some year when List.mem year reg.Registry.years ->
          let tile_count =
            Array.to_seq reg.Registry.tiles
            |> Seq.filter (fun (t : Registry.tile) -> t.year = year)
            |> Seq.fold_left (fun acc _ -> acc + 1) 0
          in
          let store_names = List.map fst store_configs in
          Stac_json.collection ~base_url ~year ~tile_count ~store_names |> respond_json
      | _ -> respond_not_found ("Collection not found: " ^ collection_id))
  | _ -> respond_not_found ("Collection not found: " ^ collection_id)

let handle_items ~base_url reg store_configs uri collection_id =
  match String.split_on_char '-' collection_id with
  | _ :: year_s :: _ -> (
      match int_of_string_opt year_s with
      | Some year when List.mem year reg.Registry.years ->
          let minx = get_float_param uri "bbox[0]" ~default:(-180.) in
          let miny = get_float_param uri "bbox[1]" ~default:(-90.) in
          let maxx = get_float_param uri "bbox[2]" ~default:180. in
          let maxy = get_float_param uri "bbox[3]" ~default:90. in
          (* Also support comma-separated bbox parameter *)
          let minx, miny, maxx, maxy =
            match get_param uri "bbox" with
            | Some bbox_str -> (
                match String.split_on_char ',' bbox_str |> List.filter_map float_of_string_opt with
                | [ a; b; c; d ] -> (a, b, c, d)
                | _ -> (minx, miny, maxx, maxy))
            | None -> (minx, miny, maxx, maxy)
          in
          let limit = get_int_param uri "limit" ~default:1000 in
          let offset = get_int_param uri "offset" ~default:0 in
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
      | _ -> respond_not_found ("Collection not found: " ^ collection_id))
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
  let minx, miny, maxx, maxy =
    match get_param uri "bbox" with
    | Some bbox_str -> (
        match String.split_on_char ',' bbox_str |> List.filter_map float_of_string_opt with
        | [ a; b; c; d ] -> (a, b, c, d)
        | _ -> (-180., -90., 180., 90.))
    | None -> (-180., -90., 180., 90.)
  in
  let limit = get_int_param uri "limit" ~default:1000 in
  let offset = get_int_param uri "offset" ~default:0 in
  let year =
    match get_param uri "datetime" with
    | Some dt -> (
        match String.split_on_char '-' dt with
        | y :: _ -> int_of_string_opt y |> Option.value ~default:2024
        | _ -> 2024)
    | None ->
        get_int_param uri "year" ~default:2024
  in
  let collections_filter =
    match get_param uri "collections" with
    | Some c -> String.split_on_char ',' c
    | None -> []
  in
  let year_matches =
    if collections_filter = [] then true
    else
      List.exists
        (fun cid ->
          match String.split_on_char '-' cid with
          | _ :: ys :: _ -> int_of_string_opt ys = Some year
          | _ -> false)
        collections_filter
  in
  if not year_matches then
    Stac_json.item_collection ~items:[] ~matched:0 ~returned:0 ~next_token:None
    |> respond_geojson
  else
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
  | _ -> respond_not_found ("Unknown path: " ^ path)

let make_callback ~base_url reg store_configs _conn req _body =
  let uri = Cohttp.Request.uri req in
  let meth = Cohttp.Request.meth req in
  Logs.debug (fun m -> m "%s %s" (Cohttp.Code.string_of_method meth) (Uri.path uri));
  match meth with
  | `GET -> route ~base_url reg store_configs uri
  | `OPTIONS -> respond_options ()
  | _ -> respond_method_not_allowed ()

let parse_store_configs () =
  (* Parse STORES env var: "name=url,name2=url2" *)
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

  (* Parse store configs from STORES env var *)
  let store_configs = parse_store_configs () in
  if store_configs = [] then (
    Logs.err (fun m -> m "STORES env var is required (e.g. STORES=okavango=url;s3=url)");
    exit 1);

  (* First store is primary — its parquet becomes the tile index.
     Other stores are loaded from their own parquet files. *)
  let stores =
    List.map (fun (name, _cfg) -> (name, data_dir ^ "/" ^ name ^ ".parquet")) store_configs
  in
  let reg = Registry.create ~stores in

  Logs.app (fun m -> m "Starting STAC server on port %d" port);
  Logs.app (fun m -> m "Base URL: %s" base_url);
  Logs.app (fun m -> m "Stores: %s" (Registry.store_names reg |> String.concat ", "));

  let callback = make_callback ~base_url reg store_configs in
  Cohttp_lwt_unix.Server.create ~mode:(`TCP (`Port port))
    (Cohttp_lwt_unix.Server.make ~callback ())
  |> Lwt_main.run
