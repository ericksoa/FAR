module RawADOExample
open System.Data
open System.Data.SqlClient

type PersonName = {Id:int;FirstName:string;LastName:string}
//raw ADO example
let retrievePeople =
  use connection = new SqlConnection("Server=localhost;Integrated security=SSPI;database=DemoData")
  connection.Open()
  use command = new SqlCommand("select id, firstname, lastname from person",connection)
  let rawResult = command.ExecuteReader()
  let people = seq { 
        while rawResult.Read() do
          yield {
            Id = rawResult.["Id"] :?> int;
            FirstName = rawResult.["FirstName"] :?> string;
            LastName = rawResult.["LastName"] :?> string
          }
      }
  people |> Seq.toList

let retrievePeopleNamed firstName =
  use connection = new SqlConnection("Server=localhost;Integrated security=SSPI;database=DemoData")
  connection.Open()
  use command = new SqlCommand("select id, firstname, lastname from person where firstname = @firstname",connection)
  let parameter = new SqlParameter("firstName",firstName)
  do command.Parameters.Add parameter |> ignore
  let rawResult = command.ExecuteReader()
  let people = seq { 
        while rawResult.Read() do
          yield {
            Id = rawResult.["Id"] :?> int;
            FirstName = rawResult.["FirstName"] :?> string;
            LastName = rawResult.["LastName"] :?> string
          }
      }
  people |> Seq.toList

let doCreateUpdateDelete() =
  use connection = new SqlConnection("Server=localhost;Integrated security=SSPI;database=DemoData")
  connection.Open()
  //create
  use createCommand = 
    new SqlCommand("insert into person (firstname,lastname) values (@firstname,@lastname)",connection)
  let firstNameParameterCreate = new SqlParameter("firstName","Aaron")
  let lastNameParameterCreate = new SqlParameter("lastName","Erickson")
  do createCommand.Parameters.Add firstNameParameterCreate |> ignore
  do createCommand.Parameters.Add lastNameParameterCreate |> ignore
  do createCommand.ExecuteNonQuery() |> ignore
  //update
  use updateCommand =
    new SqlCommand("update person set firstname=@firstname, lastname=@lastname where id=@id", connection)
  let firstNameParameterUpdate = new SqlParameter("firstName","Not")
  let lastNameParameterUpdate = new SqlParameter("lastName","Sure")
  let idParameterUpdate = new SqlParameter("id",42)
  do updateCommand.Parameters.Add firstNameParameterUpdate |> ignore
  do updateCommand.Parameters.Add lastNameParameterUpdate |> ignore
  do updateCommand.Parameters.Add idParameterUpdate |> ignore
  do updateCommand.ExecuteNonQuery() |> ignore
  //delete
  use deleteCommand = new SqlCommand("delete person where id=@id", connection)
  let idParameterDelete = new SqlParameter("id","42")
  do deleteCommand.Parameters.Add idParameterDelete |> ignore
  do deleteCommand.ExecuteNonQuery() |> ignore

