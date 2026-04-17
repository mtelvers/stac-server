let stac_version = "1.0.0"
let tile_size = 0.1

let conformance_classes =
  [
    "https://api.stacspec.org/v1.0.0/core";
    "https://api.stacspec.org/v1.0.0/ogcapi-features";
    "https://api.stacspec.org/v1.0.0/item-search";
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core";
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson";
  ]

let link ~rel ~href ?type_ ?title ?method_ () =
  let fields =
    [ ("rel", `String rel); ("href", `String href) ]
    @ (match type_ with Some t -> [ ("type", `String t) ] | None -> [])
    @ (match title with Some t -> [ ("title", `String t) ] | None -> [])
    @ (match method_ with Some m -> [ ("method", `String m) ] | None -> [])
  in
  `Assoc fields

let asset ~href ?type_ ?title ?(roles = []) ?file_size ?file_checksum () =
  let fields =
    [ ("href", `String href) ]
    @ (match type_ with Some t -> [ ("type", `String t) ] | None -> [])
    @ (match title with Some t -> [ ("title", `String t) ] | None -> [])
    @ (if roles = [] then [] else [ ("roles", `List (List.map (fun r -> `String r) roles)) ])
    @ (match file_size with Some s -> [ ("file:size", `Int s) ] | None -> [])
    @ (match file_checksum with Some c -> [ ("file:checksum", `String c) ] | None -> [])
  in
  `Assoc fields

let polygon_geometry lon lat size =
  `Assoc
    [
      ("type", `String "Polygon");
      ( "coordinates",
        `List
          [
            `List
              [
                `List [ `Float lon; `Float lat ];
                `List [ `Float (lon +. size); `Float lat ];
                `List [ `Float (lon +. size); `Float (lat +. size) ];
                `List [ `Float lon; `Float (lat +. size) ];
                `List [ `Float lon; `Float lat ];
              ];
          ] );
    ]

let landing_page ~base_url =
  `Assoc
    [
      ("type", `String "Catalog");
      ("id", `String "geotessera");
      ("title", `String "GeoTessera Embeddings");
      ( "description",
        `String
          "128-channel geospatial embeddings derived from Sentinel-1 and Sentinel-2 satellite \
           imagery using the Tessera foundation model." );
      ("stac_version", `String stac_version);
      ("conformsTo", `List (List.map (fun c -> `String c) conformance_classes));
      ( "links",
        `List
          [
            link ~rel:"self" ~href:base_url ~type_:"application/json" ();
            link ~rel:"root" ~href:base_url ~type_:"application/json" ();
            link ~rel:"data" ~href:(base_url ^ "/collections") ~type_:"application/json" ();
            link ~rel:"conformance" ~href:(base_url ^ "/conformance") ~type_:"application/json" ();
            link ~rel:"search" ~href:(base_url ^ "/search") ~type_:"application/geo+json"
              ~method_:"GET" ();
          ] );
    ]

let conformance =
  `Assoc [ ("conformsTo", `List (List.map (fun c -> `String c) conformance_classes)) ]

let collection ~base_url ~year ~tile_count =
  let year_s = string_of_int year in
  let id = "geotessera-" ^ year_s in
  `Assoc
    [
      ("type", `String "Collection");
      ("id", `String id);
      ("stac_version", `String stac_version);
      ("title", `String ("GeoTessera Embeddings " ^ year_s));
      ( "description",
        `String ("Embeddings derived from " ^ year_s ^ " Sentinel imagery") );
      ("license", `String "proprietary");
      ( "extent",
        `Assoc
          [
            ( "spatial",
              `Assoc
                [
                  ( "bbox",
                    `List
                      [
                        `List
                          [ `Float (-180.); `Float (-90.); `Float 180.; `Float 90. ];
                      ] );
                ] );
            ( "temporal",
              `Assoc
                [
                  ( "interval",
                    `List
                      [
                        `List
                          [
                            `String (year_s ^ "-01-01T00:00:00Z");
                            `String (year_s ^ "-12-31T23:59:59Z");
                          ];
                      ] );
                ] );
          ] );
      ( "summaries",
        `Assoc
          [
            ("embedding_dimensions", `List [ `Int 128 ]);
            ("sample_resolution_meters", `List [ `Int 10 ]);
            ("tile_count", `Int tile_count);
          ] );
      ( "links",
        `List
          [
            link ~rel:"self" ~href:(base_url ^ "/collections/" ^ id) ~type_:"application/json" ();
            link ~rel:"root" ~href:base_url ~type_:"application/json" ();
            link ~rel:"parent" ~href:base_url ~type_:"application/json" ();
            link ~rel:"items"
              ~href:(base_url ^ "/collections/" ^ id ^ "/items")
              ~type_:"application/geo+json" ();
          ] );
      ( "providers",
        `List
          [
            `Assoc
              [
                ("name", `String "GeoTessera");
                ("roles", `List [ `String "producer"; `String "host" ]);
              ];
          ] );
    ]

let collections_response ~base_url ~years ~tile_counts =
  let colls =
    List.map2
      (fun year count -> collection ~base_url ~year ~tile_count:count)
      years tile_counts
  in
  `Assoc
    [
      ("collections", `List colls);
      ( "links",
        `List
          [
            link ~rel:"self" ~href:(base_url ^ "/collections") ~type_:"application/json" ();
            link ~rel:"root" ~href:base_url ~type_:"application/json" ();
          ] );
    ]

let item ~base_url ~(tile : Registry.tile) ~(stores : (string * Registry.tile) list)
    ~(store_configs : (string * Registry.store_config) list) =
  let year_s = string_of_int tile.year in
  let collection_id = "geotessera-" ^ year_s in
  let lon_min = tile.lon in
  let lat_min = tile.lat in
  let lon_max = tile.lon +. tile_size in
  let lat_max = tile.lat +. tile_size in
  let grid_name = Printf.sprintf "grid_%.2f_%.2f" tile.lon tile.lat in
  let assets =
    List.filter_map
      (fun (store_name, (store_tile : Registry.tile)) ->
        match List.assoc_opt store_name store_configs with
        | None -> None
        | Some (cfg : Registry.store_config) ->
            let href = cfg.base_url ^ "/" ^ year_s ^ "/" ^ grid_name in
            let title = Printf.sprintf "%s on %s" grid_name cfg.name in
            let file_checksum =
              if store_tile.hash <> "" then Some ("sha256:" ^ store_tile.hash) else None
            in
            let file_size =
              if store_tile.file_size > 0 then Some store_tile.file_size else None
            in
            Some
              ( store_name,
                asset ~href ~title ~roles:[ "data" ] ?file_size ?file_checksum () ))
      stores
  in
  let stac_extensions =
    if List.exists (fun (_, s) -> s <> `Null) [ ("", `Null) ] then []
    else [ "https://stac-extensions.github.io/file/v2.1.0/schema.json" ]
  in
  `Assoc
    ([
       ("type", `String "Feature");
       ("stac_version", `String stac_version);
     ]
    @ (if stac_extensions = [] then []
       else
         [
           ( "stac_extensions",
             `List (List.map (fun e -> `String e) stac_extensions) );
         ])
    @ [
        ("id", `String tile.id);
        ("geometry", polygon_geometry lon_min lat_min tile_size);
        ( "bbox",
          `List [ `Float lon_min; `Float lat_min; `Float lon_max; `Float lat_max ] );
        ( "properties",
          `Assoc
            [
              ("datetime", `String (year_s ^ "-01-01T00:00:00Z"));
              ("start_datetime", `String (year_s ^ "-01-01T00:00:00Z"));
              ("end_datetime", `String (year_s ^ "-12-31T23:59:59Z"));
              ("file_size", `Int tile.file_size);
              ("scales_size", `Int tile.scales_size);
            ] );
        ( "links",
          `List
            [
              link ~rel:"self"
                ~href:
                  (base_url ^ "/collections/" ^ collection_id ^ "/items/" ^ tile.id)
                ~type_:"application/geo+json" ();
              link ~rel:"parent"
                ~href:(base_url ^ "/collections/" ^ collection_id)
                ~type_:"application/json" ();
              link ~rel:"collection"
                ~href:(base_url ^ "/collections/" ^ collection_id)
                ~type_:"application/json" ();
              link ~rel:"root" ~href:base_url ~type_:"application/json" ();
            ] );
        ("assets", `Assoc assets);
        ("collection", `String collection_id);
      ])

let item_collection ~items ~matched ~returned ~next_token =
  let fields =
    [
      ("type", `String "FeatureCollection");
      ("features", `List items);
      ("numberMatched", `Int matched);
      ("numberReturned", `Int returned);
    ]
    @ (match next_token with
       | Some t -> [ ("next", `String t) ]
       | None -> [])
  in
  `Assoc fields
