module NHibernateExample

type NHPerson() = class
  let mutable _id : int = 0
  let mutable _firstName : string = ""
  let mutable _lastName : string = ""

  abstract Id : int with get, set
  default x.Id with get() = _id and set(v) = _id <- v

  abstract FirstName : string with get, set
  default x.FirstName with get() = _firstName and set(v) = _firstName <- v

  abstract LastName : string with get, set
  default x.LastName with get() = _lastName and set(v) = _lastName <- v
end