package myMacros
import scala.annotation.{StaticAnnotation, compileTimeOnly}
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

@compileTimeOnly("enable macro paradise to expand macro annotations")
class defineFields extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro defineFieldsImpl.impl
}

object defineFieldsImpl {
  def impl(c: whitebox.Context)(annottees: c.Tree*): c.Tree = {
    import c.universe._
    
    annottees.head match {
      case q"object $objName" =>
        println(s"Expanding object: $objName")
        q"""
          object $objName {
            val index: Int = 12
          }
        """
      case annottee =>
        c.abort(annottee.pos, "Annottee must be an object")
    }
  }
}
