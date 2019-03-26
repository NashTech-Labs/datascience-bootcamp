
object BuilderExample {

  def main(args: Array[String]): Unit = {
    val builder=new HouseBuilderImpl
    val construct=builder.setNFloors(1).setNBedRooms(17).setNBathrooms(1).setColor("Cyan").build
    println(construct)
  }

  case class House(nfloors: Int, nbedRooms: Int, nbathrooms: Int, color: String) {
    override def toString(): String = { "A lovely " +color +" house with "+nbedRooms+" bedrooms and "+nbathrooms+" bathrooms." }
  }

  trait HouseBuilder {
    def setNFloors(nfloors: Int): HouseBuilder
    def setNBedRooms(nbedRooms: Int): HouseBuilder
    def setNBathrooms(nbathrooms: Int): HouseBuilder
    def setColor(color: String): HouseBuilder
    def build(): House
  }

  class HouseBuilderImpl extends HouseBuilder {
    private var nfloors: Int = 0
    private var nbedRooms: Int = 0
    private var nbathrooms: Int = 0
    private var color: String = "NONE"


    override def setNFloors(nfloors: Int) = {
      this.nfloors = nfloors
      this
    }

    override def setNBedRooms(nbedRooms: Int) = {
      this.nbedRooms = nbedRooms
      this
    }

    override def setNBathrooms(nbathrooms: Int) = {
      this.nbathrooms=nbathrooms
      this
    }

    override def setColor(color: String) = {
      this.color = color
      this
    }

    override def build=House(nfloors, nbedRooms, nbathrooms, color)
  }

}
