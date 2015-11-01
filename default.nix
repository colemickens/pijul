with (import <nixpkgs> {});

rustPlatform.buildRustPackage rec {
  name = "rust-pijul-${version}";
  version = "0.0";
  src = ./.;

  buildInputs = [ lmdb ];
  
  depsSha256 = "0g8hh29dxsq81h2zjri67jzmbg660ja3hif776vn341zknmq1n16";
  
  
}
