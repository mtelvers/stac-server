(lang dune 3.0)
(name stac_server)
(generate_opam_files true)

(package
 (name stac_server)
 (synopsis "GeoTessera STAC API server and sync tool")
 (allow_empty)
 (depends
  (ocaml (>= 5.1.0))
  dune
  arrow
  cohttp-eio
  eio_main
  yojson
  logs
  fmt))
