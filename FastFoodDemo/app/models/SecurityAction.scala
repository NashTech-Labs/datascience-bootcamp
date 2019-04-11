package models

import javax.inject.Inject
import play.api.mvc._
import play.api.mvc.Results._
import scala.concurrent.{ExecutionContext, Future}

class SecurityAction @Inject() (userService: UserRepository, parser: BodyParsers.Default)(implicit ec: ExecutionContext)
  extends ActionBuilderImpl(parser) {


  override def invokeBlock[A](request: Request[A], block: (Request[A]) => Future[Result]) = {
    /*
    val maybeUsername = request.session.get("USERNAME")
    maybeUsername match {
      case None => {
        Future.successful(Forbidden("Dude, you’re not logged in."))
      }
      case Some(u) => {
        val res: Future[Result] = block(request)
        res
      }
    }
    */
    val check=userService.checkUser(request)
    if (check==Some(1)) { println("You are logged in"); block(request) }
    else { println("You are not logged in"); Future.successful(Forbidden("You are not logged in")) }
  }

}


