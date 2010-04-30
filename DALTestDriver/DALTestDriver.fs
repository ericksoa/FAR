namespace DALTestDriver

open System
open FSharpDAL
open NUnit.Framework

type Person = {Id:int;FirstName:string;LastName:string;Age:int}
type Cat = {PetName:string;Color:string;IsCute:bool;IsMean:bool}

[<TestFixture>]
type FSharpDALTests =
  static member DefaultMasterConnectionString = "Server=localhost;Integrated security=SSPI;database=master"
  static member DefaultDataConnectionString = "Server=localhost;Integrated security=SSPI;database=PeopleData;MultipleActiveResultSets=true"
  
  [<Test>]
  static member TestDatabaseExists =
    let databaseThatShouldNotExist = Guid.NewGuid().ToString()
    let exists = DatabaseExists databaseThatShouldNotExist FSharpDALTests.DefaultMasterConnectionString
    Assert.AreEqual(false, exists)
  
  [<Test>]
  static member TestCreateDatabase =
    do DropDatabase "WeatherData" FSharpDALTests.DefaultMasterConnectionString
    do CreateDatabase "WeatherData" FSharpDALTests.DefaultMasterConnectionString
    let existsAfterCreate = DatabaseExists "WeatherData" FSharpDALTests.DefaultMasterConnectionString
    Assert.AreEqual(true, existsAfterCreate)

  [<Test>]
  static member TestCreatePeopleTable =
    do DropDatabase "WeatherData" FSharpDALTests.DefaultMasterConnectionString
    do CreateDatabase "WeatherData" FSharpDALTests.DefaultMasterConnectionString
    do CreateTableFor<Person> FSharpDALTests.DefaultDataConnectionString
    Assert.AreEqual(true, TableExistsFor<Person> FSharpDALTests.DefaultDataConnectionString)

  [<Test>]
  static member TestCreateAaronErickson =
    do DropDatabase "WeatherData" FSharpDALTests.DefaultMasterConnectionString
    do CreateDatabase "WeatherData" FSharpDALTests.DefaultMasterConnectionString
    do CreateTableFor<Person> FSharpDALTests.DefaultDataConnectionString
    use context = new ForDataContext(FSharpDALTests.DefaultDataConnectionString)
    let rowsAffected = context.Create {Id=42;FirstName="Aaron";LastName="Erickson";Age=37}
    Assert.AreEqual(1, rowsAffected)

  [<Test>]
  static member TestReadPeopleAndPets =

    do DropDatabase "PeopleData" FSharpDALTests.DefaultMasterConnectionString

    do CreateDatabase "PeopleData" FSharpDALTests.DefaultMasterConnectionString

    do CreateTableFor<Person> FSharpDALTests.DefaultDataConnectionString

    use context = new ForDataContext(FSharpDALTests.DefaultDataConnectionString)
    do context.Create {Id=1;FirstName="Aaron";LastName="Erickson";Age=37} |> ignore
    do context.Create {Id=2;FirstName="Erin";LastName="Erickson";Age=34} |> ignore
    do context.Create {Id=3;FirstName="Adriana";LastName="Erickson";Age=13} |> ignore
    do context.Create {Id=4;FirstName="Matthew";LastName="Erickson";Age=8} |> ignore

    let people = context.SequenceFrom<Person>( <@ fun p -> p.LastName = "Erickson" @> )  |> Seq.toArray

    Assert.AreEqual(people.Length,4)

    do CreateTableFor<Cat> FSharpDALTests.DefaultDataConnectionString
    do context.Create {PetName="Puppy Cat";Color="Ginger";IsCute=true;IsMean=false} |> ignore
    do context.Create {PetName="Dmitry";Color="Blue-Gray";IsCute=true;IsMean=true} |> ignore

    let theCats = context.SequenceFrom<Cat>() |> Seq.toArray

    Assert.AreEqual(theCats.Length,2)


  [<Test>]
  static member TestReadAaron =
    use context = new ForDataContext(FSharpDALTests.DefaultDataConnectionString)
    let query = <@ fun p -> p.FirstName = "Aaron" @> 
    let people = context.SequenceFrom<Person>( query )  |> Seq.toArray
    Assert.AreEqual(people.Length,1)

  [<Test>]
  static member TestReadAaronErickson =
    use context = new ForDataContext(FSharpDALTests.DefaultDataConnectionString)
    let query = <@ fun p -> p.FirstName = "Aaron" && p.LastName = "Erickson" @> 
    let people = context.SequenceFrom<Person>( query )  |> Seq.toArray
    Assert.AreEqual(people.Length,1)

  [<Test>]
  static member TestReadSuperCompound =
    use context = new ForDataContext(FSharpDALTests.DefaultDataConnectionString)
    let query = <@ fun(p:Person) -> (p.FirstName = "Aaron" && p.LastName = "Erickson") || (p.FirstName = "Ted" && p.LastName = "Neward") @> 
    let people = context.SequenceFrom<Person>( query )  |> Seq.toArray
    Assert.AreEqual(people.Length,2)