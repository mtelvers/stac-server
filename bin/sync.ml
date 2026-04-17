open Stac_lib

let setup_logging () =
  Fmt_tty.setup_std_outputs ();
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level (Some Logs.Info)

let usage () =
  Printf.eprintf
    {|Usage: sync <command> [options]

Commands:
  ingest <parquet-file> <store-name>
    Load a parquet file as a store manifest.
    The parquet must have at least lat, lon, year columns.

  diff <source-store> <target-store>
    Show tiles present in source but missing from target.

  stale <source-store> <target-store>
    Show tiles where target hash differs from source.

  manifest <source-store> <target-store> [--output file]
    Generate a copy manifest (one path per line) for
    tiles missing from target.

  record <store-name> <manifest-file>
    After copying, mark tiles from manifest as synced in
    the target store. Rewrites the store's parquet file.

  scan <store-name> <listing-file>
    Build a store manifest from a file listing. Lines should
    include file sizes and .npy paths. The scanner extracts
    grid coordinates, year, and file sizes, distinguishing
    embeddings from scales files (*_scales.npy).

    Example:
      find /mnt/cephfs/... -name '*.npy' -printf '%%s %%p\n' > listing.txt
      s5cmd ls 's3://bucket/.../*/*/*.npy' > listing.txt
      sync scan scaleway listing.txt

  check [store-name]
    Check data quality — reports tiles with zero file_size
    or scales_size. If no store is given, checks all stores.

  status
    Show summary of all stores and their tile counts.

Environment:
  DATA_DIR   Directory for parquet files (default: ./data)

Store parquet files are expected at DATA_DIR/<store-name>.parquet.
The first store listed in any command that loads all stores is the primary.
|};
  exit 1

let data_dir () = Sys.getenv_opt "DATA_DIR" |> Option.value ~default:"./data"

let fold_lines ic f init =
  let rec loop acc =
    match input_line ic with
    | line -> loop (f acc (String.trim line))
    | exception End_of_file -> close_in ic; acc
  in
  loop init

let load_reg () =
  let dir = data_dir () in
  let stores =
    match Sys.readdir dir with
    | files ->
        files |> Array.to_list
        |> List.filter (fun f -> Filename.check_suffix f ".parquet")
        |> List.sort compare
        |> List.map (fun f -> (Filename.chop_suffix f ".parquet", dir ^ "/" ^ f))
    | exception Sys_error _ -> []
  in
  if stores = [] then (
    Logs.err (fun m -> m "No parquet files found in %s" dir);
    exit 1);
  Registry.create ~stores

(* -- Listing parsers -- *)

(* Split on whitespace, take last two tokens as (size, path) *)
let parse_listing_line line =
  let tokens = String.split_on_char ' ' line |> List.filter (( <> ) "") in
  match List.rev tokens with
  | path :: size_str :: _ ->
      Option.bind (int_of_string_opt size_str) (fun size ->
          if size > 0 then Some (size, path) else None)
  | _ -> None

(* Extract year, lon, lat, is_scales from a path containing year/grid_lon_lat *)
let parse_path path =
  let parts = String.split_on_char '/' path in
  let is_scales = String.length path >= 11 &&
                  String.sub path (String.length path - 11) 11 = "_scales.npy" in
  let parse_grid_dir dir =
    let s = String.sub dir 5 (String.length dir - 5) in
    let rec find_last_us i = if i < 0 then 0 else if s.[i] = '_' then i else find_last_us (i - 1) in
    let us = find_last_us (String.length s - 1) in
    match float_of_string_opt (String.sub s 0 us),
          float_of_string_opt (String.sub s (us + 1) (String.length s - us - 1)) with
    | Some lon, Some lat -> Some (lon, lat)
    | _ -> None
  in
  let rec find = function
    | year_s :: grid_dir :: _
      when String.length grid_dir >= 5 && String.sub grid_dir 0 5 = "grid_" ->
        Option.bind (int_of_string_opt year_s) (fun year ->
            if year >= 2000 && year <= 2100 then
              Option.map (fun (lon, lat) -> (year, lon, lat, is_scales)) (parse_grid_dir grid_dir)
            else
              find (grid_dir :: []))
    | _ :: rest -> find rest
    | [] -> None
  in
  find parts

let path_to_tile_id line =
  match parse_path (String.trim line) with
  | Some (year, lon, lat, _) -> Some (Registry.tile_id_of_coords ~year lon lat)
  | None ->
      let s = String.trim line in
      if s <> "" then Some s else None

(* -- Commands -- *)

let cmd_ingest = function
  | [ parquet_path; store_name ] ->
      let tiles = Registry.load_parquet parquet_path in
      Registry.save_parquet (data_dir () ^ "/" ^ store_name ^ ".parquet") tiles
  | _ ->
      Printf.eprintf "Usage: sync ingest <parquet-file> <store-name>\n";
      exit 1

let cmd_diff = function
  | [ source; target ] ->
      let reg = load_reg () in
      let missing = Registry.missing_from_store reg ~source_store:source ~target_store:target in
      let stale = Registry.stale_in_store reg ~source_store:source ~target_store:target in
      Printf.printf "Source: %s\nTarget: %s\n" source target;
      Printf.printf "Missing from target: %d tiles\n" (Array.length missing);
      Printf.printf "Stale in target: %d tiles\n" (Array.length stale);
      if Array.length missing > 0 then (
        Printf.printf "\nFirst 10 missing:\n";
        Array.to_seq missing |> Seq.take 10
        |> Seq.iter (fun (t : Registry.tile) ->
               Printf.printf "  %s (year=%d, hash=%s)\n" t.id t.year
                 (String.sub t.hash 0 (min 12 (String.length t.hash)))))
  | _ ->
      Printf.eprintf "Usage: sync diff <source-store> <target-store>\n";
      exit 1

let cmd_manifest args =
  let rec parse_args output_file stores = function
    | ("--output" | "-o") :: f :: rest -> parse_args (Some f) stores rest
    | s :: rest -> parse_args output_file (s :: stores) rest
    | [] -> (output_file, List.rev stores)
  in
  let output_file, stores = parse_args None [] args in
  match stores with
  | [ source; target ] ->
      let reg = load_reg () in
      let missing = Registry.missing_from_store reg ~source_store:source ~target_store:target in
      let oc = match output_file with Some f -> open_out f | None -> stdout in
      Array.iter
        (fun (t : Registry.tile) ->
          Printf.fprintf oc "%d/grid_%.2f_%.2f\n" t.year t.lon t.lat)
        missing;
      Option.iter
        (fun f -> close_out oc;
                  Printf.eprintf "Wrote %d paths to %s\n%!" (Array.length missing) f)
        output_file
  | _ ->
      Printf.eprintf "Usage: sync manifest <source-store> <target-store> [-o file]\n";
      exit 1

let cmd_record = function
  | [ store_name; manifest_file ] ->
      let reg = load_reg () in
      let tile_ids =
        fold_lines (open_in manifest_file)
          (fun acc line ->
            if line = "" then acc
            else match path_to_tile_id line with Some id -> id :: acc | None -> acc)
          []
        |> List.rev
      in
      Logs.info (fun m -> m "Recording %d tiles as synced to %s" (List.length tile_ids) store_name);
      let entries =
        match Hashtbl.find_opt reg.stores store_name with
        | Some e -> e
        | None ->
            let e = Hashtbl.create 1000 in
            Hashtbl.replace reg.stores store_name e;
            e
      in
      let added =
        List.fold_left
          (fun count tile_id ->
            match Registry.tile_by_id reg tile_id with
            | Some tile -> Hashtbl.replace entries tile_id tile; count + 1
            | None ->
                Logs.warn (fun m -> m "Tile %s not found in registry, skipping" tile_id);
                count)
          0 tile_ids
      in
      let all_tiles = Hashtbl.fold (fun _k t acc -> t :: acc) entries [] |> Array.of_list in
      Registry.save_parquet (data_dir () ^ "/" ^ store_name ^ ".parquet") all_tiles;
      Logs.app (fun m -> m "Recorded %d tiles, total %d in %s" added (Hashtbl.length entries) store_name)
  | _ ->
      Printf.eprintf "Usage: sync record <store-name> <manifest-file>\n";
      exit 1

type scan_acc = { matched : int; skipped : int; lines : int }

let cmd_scan = function
  | [ store_name; listing_file ] ->
      let _reg = load_reg () in
      let tile_data : (string, float * float * int * int * int) Hashtbl.t = Hashtbl.create 10000 in
      let { matched; skipped; lines } =
        fold_lines (open_in listing_file)
          (fun acc line ->
            let lines = acc.lines + 1 in
            if line = "" then { acc with lines }
            else
              match parse_listing_line line with
              | None -> { acc with lines; skipped = acc.skipped + 1 }
              | Some (size, path) ->
                  match parse_path path with
                  | None -> { acc with lines; skipped = acc.skipped + 1 }
                  | Some (year, lon, lat, is_scales) ->
                      let tile_id = Registry.tile_id_of_coords ~year lon lat in
                      let _lon, _lat, _year, cur_fs, cur_ss =
                        Hashtbl.find_opt tile_data tile_id
                        |> Option.value ~default:(lon, lat, year, 0, 0)
                      in
                      let fs = if is_scales then cur_fs else max cur_fs size in
                      let ss = if is_scales then max cur_ss size else cur_ss in
                      Hashtbl.replace tile_data tile_id (lon, lat, year, fs, ss);
                      { acc with lines; matched = acc.matched + 1 })
          { matched = 0; skipped = 0; lines = 0 }
      in
      Logs.app (fun m -> m "Scanned %d lines: %d matched, %d skipped" lines matched skipped);
      let entries =
        Hashtbl.fold
          (fun tile_id (lon, lat, year, fs, ss) acc ->
            Registry.{ id = tile_id; lat; lon; year; file_size = fs; scales_size = ss; hash = "" }
            :: acc)
          tile_data []
        |> Array.of_list
      in
      Registry.save_parquet (data_dir () ^ "/" ^ store_name ^ ".parquet") entries
  | _ ->
      Printf.eprintf "Usage: sync scan <store-name> <listing-file>\n";
      exit 1

let check_store ~store_name tiles =
  let bad =
    Array.to_seq tiles
    |> Seq.filter (fun (t : Registry.tile) -> t.file_size = 0 || t.scales_size = 0)
    |> List.of_seq
  in
  match bad with
  | [] ->
      Printf.printf "  %s: OK (%d tiles, no issues)\n" store_name (Array.length tiles)
  | _ ->
      Printf.printf "  %s: %d tiles, %d with issues:\n" store_name (Array.length tiles) (List.length bad);
      Printf.printf "    %-45s %12s %12s\n" "tile" "file_size" "scales_size";
      Printf.printf "    %s\n" (String.make 69 '-');
      List.iter
        (fun (t : Registry.tile) ->
          Printf.printf "    %-45s %12d %12d\n"
            (Printf.sprintf "%d/grid_%.2f_%.2f" t.year t.lon t.lat)
            t.file_size t.scales_size)
        bad

let cmd_check = function
  | [] ->
      let reg = load_reg () in
      Printf.printf "\n=== Data Quality Check ===\n\n";
      Registry.store_names reg
      |> List.iter (fun name ->
             match Hashtbl.find_opt reg.stores name with
             | Some entries ->
                 let tiles = Hashtbl.fold (fun _k t acc -> t :: acc) entries [] |> Array.of_list in
                 check_store ~store_name:name tiles
             | None -> ())
  | [ store_name ] ->
      let reg = load_reg () in
      (match Hashtbl.find_opt reg.stores store_name with
      | Some entries ->
          let tiles = Hashtbl.fold (fun _k t acc -> t :: acc) entries [] |> Array.of_list in
          check_store ~store_name tiles
      | None -> Logs.err (fun m -> m "Store %s not found" store_name))
  | _ ->
      Printf.eprintf "Usage: sync check [store-name]\n";
      exit 1

let cmd_status () =
  let reg = load_reg () in
  Printf.printf "\n=== GeoTessera Sync Status ===\n\n";
  Printf.printf "Registry: %d tiles across %d year(s)\n" (Array.length reg.tiles) (List.length reg.years);
  Printf.printf "Years: %s\n\n" (reg.years |> List.map string_of_int |> String.concat ", ");
  let stores = Registry.store_names reg in
  Printf.printf "Stores:\n";
  List.iter
    (fun name ->
      match Hashtbl.find_opt reg.stores name with
      | Some entries -> Printf.printf "  %-15s %d tiles\n" name (Hashtbl.length entries)
      | None -> Printf.printf "  %-15s (not loaded)\n" name)
    stores;
  Printf.printf "\nPairwise diffs (missing from target):\n";
  List.iter
    (fun source ->
      List.iter
        (fun target ->
          if source <> target then
            let missing = Registry.missing_from_store reg ~source_store:source ~target_store:target in
            if Array.length missing > 0 then
              Printf.printf "  %s -> %s: %d missing\n" source target (Array.length missing))
        stores)
    stores;
  let primary_bad =
    Array.fold_left
      (fun acc (t : Registry.tile) ->
        if t.file_size = 0 || t.scales_size = 0 then acc + 1 else acc)
      0 reg.tiles
  in
  if primary_bad > 0 then
    Printf.printf "\nWarning: %d tiles with zero file_size or scales_size (run 'sync check' for details)\n"
      primary_bad

let () =
  setup_logging ();
  match Array.to_list Sys.argv |> List.tl with
  | "ingest" :: args -> cmd_ingest args
  | "diff" :: args -> cmd_diff args
  | "manifest" :: args -> cmd_manifest args
  | "record" :: args -> cmd_record args
  | "scan" :: args -> cmd_scan args
  | "check" :: args -> cmd_check args
  | [ "status" ] -> cmd_status ()
  | _ -> usage ()
