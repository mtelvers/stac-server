let src = Logs.Src.create "registry" ~doc:"Tile registry"
module Log = (val Logs.src_log src)

type tile = {
  id : string;
  lat : float;
  lon : float;
  year : int;
  file_size : int;
  scales_size : int;
  hash : string;
}

type store_config = {
  name : string;
  base_url : string;
  path_pattern : string;
}

type t = {
  tiles : tile array;
  years : int list;
  tile_index : (string, int) Hashtbl.t;
  stores : (string, (string, tile) Hashtbl.t) Hashtbl.t;
}

let tile_id_of_coords ~year lon lat = Printf.sprintf "%d_grid_%.2f_%.2f" year lon lat
let tile_size = 0.1

let load_parquet parquet_path =
  Log.info (fun m -> m "Loading %s" parquet_path);
  let table = Arrow.Parquet_reader.table parquet_path in
  let num_rows = Arrow.Table.num_rows table in
  Log.info (fun m -> m "  %d rows" num_rows);
  let lats = Arrow.Wrapper.Column.read_float table ~column:(`Name "lat") in
  let lons = Arrow.Wrapper.Column.read_float table ~column:(`Name "lon") in
  let years_col = Arrow.Wrapper.Column.read_int table ~column:(`Name "year") in
  let file_sizes =
    try Arrow.Wrapper.Column.read_int table ~column:(`Name "file_size")
    with _ -> Array.make num_rows 0
  in
  let scales_sizes =
    try Arrow.Wrapper.Column.read_int table ~column:(`Name "scales_size")
    with _ -> Array.make num_rows 0
  in
  let hashes =
    try Arrow.Wrapper.Column.read_large_utf8_opt table ~column:(`Name "hash")
    with _ ->
      try
        let h = Arrow.Wrapper.Column.read_utf8 table ~column:(`Name "hash") in
        Array.map (fun s -> Some s) h
      with _ -> Array.make num_rows None
  in
  Array.init num_rows (fun i ->
      {
        id = tile_id_of_coords ~year:years_col.(i) lons.(i) lats.(i);
        lat = lats.(i);
        lon = lons.(i);
        year = years_col.(i);
        file_size = file_sizes.(i);
        scales_size = scales_sizes.(i);
        hash = Option.value ~default:"" hashes.(i);
      })

let save_parquet parquet_path tiles =
  let n = Array.length tiles in
  let lats = Array.map (fun (t : tile) -> t.lat) tiles in
  let lons = Array.map (fun (t : tile) -> t.lon) tiles in
  let years = Array.map (fun (t : tile) -> t.year) tiles in
  let file_sizes = Array.map (fun (t : tile) -> t.file_size) tiles in
  let scales_sizes = Array.map (fun (t : tile) -> t.scales_size) tiles in
  let hashes = Array.map (fun (t : tile) -> t.hash) tiles in
  let table =
    Arrow.Table.create
      [
        Arrow.Table.col lats Arrow.Table.Float ~name:"lat";
        Arrow.Table.col lons Arrow.Table.Float ~name:"lon";
        Arrow.Table.col years Arrow.Table.Int ~name:"year";
        Arrow.Table.col file_sizes Arrow.Table.Int ~name:"file_size";
        Arrow.Table.col scales_sizes Arrow.Table.Int ~name:"scales_size";
        Arrow.Table.col hashes Arrow.Table.Utf8 ~name:"hash";
      ]
  in
  Arrow.Table.write_parquet table parquet_path;
  Log.info (fun m -> m "Wrote %d tiles to %s" n parquet_path)

let tiles_to_hashtbl tiles =
  let h = Hashtbl.create (Array.length tiles) in
  Array.iter (fun (t : tile) -> Hashtbl.replace h t.id t) tiles;
  h

let create ~stores =
  match stores with
  | [] -> failwith "At least one store is required"
  | (primary_name, primary_path) :: rest ->
      let primary_tiles = load_parquet primary_path in
      let tile_index = Hashtbl.create (Array.length primary_tiles) in
      Array.iteri (fun i (t : tile) -> Hashtbl.replace tile_index t.id i) primary_tiles;
      let years =
        Array.to_seq primary_tiles
        |> Seq.map (fun (t : tile) -> t.year)
        |> Seq.fold_left (fun acc y -> if List.mem y acc then acc else y :: acc) []
        |> List.sort compare
      in
      Log.info (fun m ->
          m "Primary store: %s (%d tiles, years: %s)" primary_name
            (Array.length primary_tiles)
            (years |> List.map string_of_int |> String.concat ", "));
      let store_table = Hashtbl.create (1 + List.length rest) in
      Hashtbl.replace store_table primary_name (tiles_to_hashtbl primary_tiles);
      List.iter
        (fun (name, path) ->
          if Sys.file_exists path then (
            let tiles = load_parquet path in
            Log.info (fun m -> m "  %s: %d tiles" name (Array.length tiles));
            Hashtbl.replace store_table name (tiles_to_hashtbl tiles))
          else (
            Log.warn (fun m -> m "  %s: no manifest (%s not found)" name path);
            Hashtbl.replace store_table name (Hashtbl.create 0)))
        rest;
      { tiles = primary_tiles; years; tile_index; stores = store_table }

let tile_by_id reg id = Hashtbl.find_opt reg.tile_index id |> Option.map (fun i -> reg.tiles.(i))

let stores_for_tile reg tile_id =
  Hashtbl.fold
    (fun store_name entries acc ->
      match Hashtbl.find_opt entries tile_id with
      | None -> acc
      | Some tile -> (store_name, tile) :: acc)
    reg.stores []

let missing_from_store reg ~source_store ~target_store =
  match Hashtbl.find_opt reg.stores source_store with
  | None ->
      Log.err (fun m -> m "Source store %s not loaded" source_store);
      [||]
  | Some source_entries ->
      let target_entries =
        Hashtbl.find_opt reg.stores target_store
        |> Option.value ~default:(Hashtbl.create 0)
      in
      Hashtbl.fold
        (fun tile_id tile acc ->
          if Hashtbl.mem target_entries tile_id then acc else tile :: acc)
        source_entries []
      |> Array.of_list

let stale_in_store reg ~source_store ~target_store =
  match
    ( Hashtbl.find_opt reg.stores source_store,
      Hashtbl.find_opt reg.stores target_store )
  with
  | None, _ | _, None -> [||]
  | Some source, Some target ->
      Hashtbl.fold
        (fun tile_id src_tile acc ->
          match Hashtbl.find_opt target tile_id with
          | Some tgt_tile
            when src_tile.hash <> "" && tgt_tile.hash <> ""
                 && tgt_tile.hash <> src_tile.hash ->
              (src_tile, tgt_tile) :: acc
          | _ -> acc)
        source []
      |> Array.of_list

let store_names reg =
  Hashtbl.fold (fun name _ acc -> name :: acc) reg.stores [] |> List.sort compare

let tiles_in_bbox reg ~year ~minx ~miny ~maxx ~maxy =
  Array.to_seq reg.tiles
  |> Seq.filter (fun (t : tile) ->
         t.year = year && t.lon < maxx
         && t.lon +. tile_size > minx
         && t.lat < maxy
         && t.lat +. tile_size > miny)
