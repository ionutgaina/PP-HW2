import util.Util.{Line, Row}
//import TestTables.tableImperative
//import TestTables.tableFunctional
//import TestTables.tableObjectOriented

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)

  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}
case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    r.get(colName) match {
    case Some(value) => Some(predicate(value))
    case None => None
  }
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    Some(f1.eval(r).contains(true) && f2.eval(r).contains(true))
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    if (f1.eval(r).isEmpty || f2.eval(r).isEmpty)
      None;
    else
      Some(f1.eval(r).contains(true) || f2.eval(r).contains(true))
  }
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = ???
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] = ???
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] = ???
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] = ???
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = ???
}


class Table (columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames : Line = columnNames
  def getTabular : List[List[String]] = tabular

  // 1.1
  override def toString: String =
    columnNames.mkString(",") + "\n" +
      tabular.map(_.mkString(",")).mkString("\n")

  // 2.1
  def select(columns: Line): Option[Table] = {
    def selectColumn(column: String): Option[Line] = {
      val index = columnNames.indexOf(column)
      if (index == -1) None
      else Some(tabular.map(_(index)))
    }

    val selectedColumns = columns.map(selectColumn).filter(_.isDefined).map(_.get)

    if (selectedColumns.isEmpty) None
    else Some(new Table(columns, selectedColumns.transpose))

  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
    // tabular.indices <=> (0 until tabular.size)
    val mappedRows = tabular.indices.map(i => columnNames.zip(tabular(i)).toMap).toList
    val filteredRows = mappedRows.filter(cond.eval(_).contains(true))
    if (filteredRows.isEmpty) None else Some(new Table(columnNames, filteredRows.map(_.values.toList)))
  }

  // 2.3.
  def newCol(name: String, defaultVal: String): Table =
    new Table(columnNames :+ name, tabular.map(_ :+ defaultVal))

  // 2.4.
  def merge(key: String, other: Table): Option[Table] = ???
}

object Table {
  // 1.2
  def apply(s: String): Table = {
    val lines = s.split("\n").toList
    val columnNames = lines.head.split(",").toList;
    val tabular = lines.tail.map(_.split(",").toList);
    new Table(columnNames, tabular)
  }
}
