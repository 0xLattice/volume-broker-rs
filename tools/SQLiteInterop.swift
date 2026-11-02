import Foundation
import SQLite3
@main struct Interop {
 static func main() throws {
  let connection=try SQLiteConnection(path:CommandLine.arguments[1])
  if CommandLine.arguments[2] == "create" {
   let root=CommandLine.arguments[3]
   try connection.transaction {
    try connection.exec("INSERT INTO cas_data VALUES('\(root)',X'68656c6c6f')")
    try connection.exec("INSERT INTO volume_metadata VALUES('\(root)',1)")
    try connection.exec("INSERT INTO volume_entries VALUES('\(root)','\(root)')")
    try connection.exec("INSERT INTO retained_roots VALUES('swift','\(root)')")
   }
  } else {
   var statement:OpaquePointer?
   precondition(sqlite3_prepare_v2(connection.db,"SELECT count(*) FROM cas_data WHERE data IN (X'68656c6c6f',X'776f726c64')",-1,&statement,nil)==SQLITE_OK)
   defer { sqlite3_finalize(statement) }
   precondition(sqlite3_step(statement)==SQLITE_ROW)
   precondition(sqlite3_column_int(statement,0)==2)
   print("Swift opened Rust-modified schema v2 and verified both content rows")
  }
 }
}
