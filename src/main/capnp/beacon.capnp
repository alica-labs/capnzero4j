using Java = import "/java.capnp";

$Java.package("de.unikassel.vs.pdDebug");
$Java.outerClassname("Beacon");

@0x8c1726f48700b030;

struct BeaconStruct {
  uuid @0 :Data;
  ip @1 :Text;
  port @2 :Int16;
}