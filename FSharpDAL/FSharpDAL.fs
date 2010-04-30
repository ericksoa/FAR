module FSharpDAL
open System
open System.Reflection
open System.Data.SqlClient
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.DerivedPatterns 

let MakeConnection connectionString =
  new SqlConnection(connectionString)

let MakeCommand commandText connection =
  new SqlCommand(commandText, connection)

let MakeCreateScript databaseName =
  sprintf "create database %s" databaseName 

let CreateDatabase databaseName connectionString =
  let createScript = databaseName |> MakeCreateScript
  use connection = connectionString |> MakeConnection
  use createCommand = MakeCommand createScript connection
  do connection.Open() |> ignore
  do createCommand.ExecuteNonQuery() |> ignore

let MakeExistsScript databaseName =
  sprintf "if db_id('%s') is NULL select 0 else select 1" databaseName

let DatabaseExists databaseName connectionString =
  let existsScript = databaseName |> MakeExistsScript
  use connection = connectionString |> MakeConnection
  use existsCommand = MakeCommand existsScript connection
  do connection.Open() |> ignore
  existsCommand.ExecuteScalar() :?> int = 1 

let MakeDropScript databaseName =
  sprintf "drop database %s" databaseName

let DropDatabase databaseName connectionString =
  if DatabaseExists databaseName connectionString then
    let dropScript = databaseName |> MakeDropScript
    use connection = connectionString |> MakeConnection
    use dropCommand = MakeCommand dropScript connection
    do connection.Open() |> ignore
    do dropCommand.ExecuteNonQuery() |> ignore

//Thanks Mark Needham for this routine
let ConvertToCommaSeparatedString (value:seq<string>) =
    let rec convert (innerVal:List<string>) acc = 
        match innerVal with
            | [] -> acc
            | hd::[] -> convert [] (acc + hd)
            | hd::tl -> convert tl (acc + hd + ",")           
    convert (Seq.toList value) ""

let FSharpTypeToSqlType fSharpType size =
  match fSharpType, size with
  | t, Some(s) when t = typeof<string> -> sprintf "varchar(%i)" s
  | t, _ when t = typeof<string> -> "varchar(255)"
  | t, _ when t = typeof<int> -> "int"
  | t, _ when t = typeof<bool> -> "bit"
  | t, _ when t = typeof<DateTime> -> "datetime"
  | _, _ -> raise( new NotSupportedException() )

let private CreateTable tableName columns connectionString =
  let columnList =
    columns 
    |> Seq.map (fun (name, colType, size) -> sprintf "%s %s" name (FSharpTypeToSqlType colType size))
    |> ConvertToCommaSeparatedString
  let query = sprintf "create table %s (%s)" tableName columnList
  use connection = connectionString |> MakeConnection
  use createCommand = MakeCommand query connection
  do connection.Open() |> ignore
  do createCommand.ExecuteNonQuery() |> ignore

let CreateTableFor<'a> connectionString =
  let tableName = typeof<'a>.Name
  let columnSpecSelector (p:PropertyInfo) = (p.Name,p.PropertyType,None)
  let columnSpecs = typeof<'a>.GetProperties() |> Seq.map columnSpecSelector
  do CreateTable tableName columnSpecs connectionString

let TableExistsFor<'a> connectionString =
  let tableName = typeof<'a>.Name
  let query = 
    sprintf 
      "SELECT COUNT(*) FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].%s') AND type in (N'U')"
      tableName
  use connection = connectionString |> MakeConnection
  use existsCommand = MakeCommand query connection
  do connection.Open() |> ignore
  existsCommand.ExecuteScalar() :?> int = 1 

let makeSimpleSelect table fields =
  let commaSeperatedFields = fields |> ConvertToCommaSeparatedString
  sprintf "select %s from %s" commaSeperatedFields table

type ParseNode = 
  | EqualNode of Type list * Expr list
  | AndNode of ParseNode option * ParseNode option
  | OrNode of ParseNode option * ParseNode option

type ForDataContext =
  val connection : SqlConnection
  new(connectionString) = { connection = connectionString |> MakeConnection }
  with
    interface IDisposable with
      member disposable.Dispose() =
        disposable.connection.Close()

    member private context.OpenConnectionIfNeeded() =
      if (context.connection.State <> Data.ConnectionState.Open) then context.connection.Open()
    //core routine that takes sql + parameters and yields readers that
    //  eventually get composed into records we want to work with
    member private context.DoQuery query (rawParameters:array<string * #obj>) =
      use command = MakeCommand query context.connection
      do rawParameters 
        |> Array.map( fun r -> new SqlParameter(fst r,snd r) )
        |> Array.iter( fun p -> command.Parameters.Add p |> ignore )
      do context.OpenConnectionIfNeeded()
      let reader = command.ExecuteReader()
      seq { while reader.Read() do yield reader }

    member private context.DoCommand command (rawParameters:array<string * #obj>) =
      use command = MakeCommand command context.connection
      do rawParameters 
        |> Array.map( fun r -> new SqlParameter(fst r,snd r) )
        |> Array.iter( fun p -> command.Parameters.Add p |> ignore )
      do context.OpenConnectionIfNeeded()
      command.ExecuteNonQuery()

    member context.Create someObject =
      let tableName = someObject.GetType().Name
      let columnsAndValues = 
        someObject.GetType().GetProperties() |> Seq.map( fun p -> (p.Name,p.GetValue(someObject,Array.empty)))
      let columnNames columnsValuePair = 
        columnsValuePair 
        |> Seq.map( fun pair -> fst pair) 
        |> ConvertToCommaSeparatedString
      let convertToValueParameterBucket columnsValuePair =
        columnsValuePair
        |> Seq.map( fun pair -> sprintf "@%s" (fst pair))
        |> ConvertToCommaSeparatedString
      let query = 
        sprintf 
          "insert into %s (%s) values (%s)"
          tableName 
          (columnsAndValues |> columnNames) 
          (columnsAndValues |> convertToValueParameterBucket)
      let parameters = columnsAndValues |> Seq.toArray
      context.DoCommand query parameters

    //simple case where we are getting all the rows from a table
    member context.SequenceFrom<'a>() =
      context.SequenceFrom<'a>(String.Empty,Array.empty)
     
    //core routine that composes and executes the query
    member private context.SequenceFrom<'a>((whereClause:string),(parameters:array<string * #obj>)) =
      let tableName = typeof<'a>.Name
      let memberNameSelector (m:#MemberInfo) = m.Name
      let propertyNames = typeof<'a>.GetProperties() |> Seq.map memberNameSelector
      let query = (makeSimpleSelect tableName propertyNames) + " " + whereClause
      let creator = Reflection.FSharpValue.PreComputeRecordConstructor(typeof<'a>, BindingFlags.Public)
      let data = context.DoQuery query parameters
      let readObjectsFromReaderByField (reader:SqlDataReader) (keys:seq<string>) =
        keys |> Seq.map( fun(k) -> reader.[k] ) |> Seq.toArray
      data |> Seq.map( fun r -> creator(readObjectsFromReaderByField r propertyNames) :?> 'a )     

    //takes a predicate and generates a tuple composed of
    //  parameterized sql * parameters
    static member private ParseCriteria<'a> (criteria:Expr<'a -> bool>) =

      let rec predicateParser expr =
        match expr with
          | SpecificCall <@ (=) @> (optionExpr, types, exprs) -> Some(EqualNode(types,exprs))
          | SpecificCall <@ (&&) @> (optionExpr, types, exprs) 
            -> Some(AndNode(predicateParser(exprs.[0]),predicateParser(exprs.[1])))
          | SpecificCall <@ (||) @> (optionExpr, types, exprs)
            -> Some(OrNode(predicateParser(exprs.[0]),predicateParser(exprs.[1])))
          | Patterns.IfThenElse (left, middle, right) 
            -> match middle with
               | Patterns.Call(optionExpr, types, exprs) 
                 -> Some(AndNode(predicateParser(left), predicateParser(middle)))
               | Patterns.Value(value, valueType) 
                 -> Some(OrNode(predicateParser(left), predicateParser(right)))
               | _ -> None
            | _ -> None

      let parsedResult = 
        match criteria with
          | Patterns.Lambda(var,lambda) -> predicateParser lambda
          | _ -> None

      let paramEnumerator =
        let paramNames = 1 |> Seq.unfold (fun i -> Some(i+1,i)) |> Seq.map( fun i -> sprintf "param%i" i)
        paramNames.GetEnumerator()

      //let paramEnumerator = 
      //  let paramNames = {1..10000} |> Seq.map (fun i -> sprintf "param%i" i)
      //  paramNames.GetEnumerator()

      let nextParam() = 
        paramEnumerator.MoveNext() |> ignore
        paramEnumerator.Current 
      
      let paramList = new System.Collections.Generic.List<string * obj>()

      let rec queryString (treeNode:ParseNode option) =
        match treeNode with
        | Some(node) -> 
          match node with
          | EqualNode (left,right) ->
            match (right.[0],right.[1]) with
            | (Patterns.PropertyGet (option,property,someList), Patterns.Value(value, valueType)) ->
              let p = nextParam()
              do (p,value) |> paramList.Add 
              sprintf "%s = @%s" property.Name p
            | _ -> raise(new InvalidOperationException())
          | AndNode (left,right) -> sprintf "(%s and %s)" (queryString left) (queryString right)
          | OrNode (left,right) -> sprintf "(%s or %s)" (queryString left) (queryString right)
        | None -> ""

      queryString parsedResult, paramList.ToArray()

    //Performs queries where you have (for now) simple object.Property = someValue
    member context.SequenceFrom<'a> (criteria:Expr<'a -> bool>) =
      let queryAndParams = criteria |> ForDataContext.ParseCriteria
      context.SequenceFrom<'a> ((sprintf "where %s" (fst queryAndParams)),(snd queryAndParams))
  