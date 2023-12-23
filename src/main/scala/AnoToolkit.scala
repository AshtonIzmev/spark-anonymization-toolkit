import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Random


object AnoToolkit {

  import DataFrameMaskingImplicits.DataFrameMaskingImplicits
  import DataFrameImplicits.DataFrameImplicits

  private val SECRET_LENGTH = 128
  var RANDOM_SECRET: String = Random.nextString(SECRET_LENGTH)

  def applyMasks(maskPaths: Seq[String], outPath:String, dataLoader: (String, String) => DataFrame): Unit = {
    maskPaths
      .map(PolicyLoader.loadObject)
      .flatMap(o => o.tables.map(t => (o.database, t)))
      .foreach(e => dataLoader(e._1, e._2.table.toLowerCase)
        .limitOrAll(e._2.limit)
        .anonymise(e._2.columns)
        .writeToOrc(s"${outPath}/${e._1}_${e._2.table}.orc")
      )
  }

  private def getModulo(s:String, len:Int): Int = {
    val hash: Int = if (s == null) Random.nextInt() else s.hashCode
    val remain = hash % len
    if (remain > 0) return remain
    remain + len
  }

  val getFakeFirstname: UserDefinedFunction = udf { (s: String) =>
    firstnames(getModulo(s, firstnames.length))
  }

  val getFakeLastname: UserDefinedFunction = udf { (s: String) =>
    lastnames(getModulo(s, lastnames.length))
  }

  val getFakeCorpname: UserDefinedFunction = udf { (s: String) =>
    lastnames(getModulo(s, lastnames.length))
  }

  val firstnames = Seq(
    "Case", "Jaqueline", "Aracely", "Hayden", "Cornelius", "Aidan", "Isla", "Amiya", "Brielle", "Damaris",
    "Roselyn", "Chance", "Madilyn", "Ellen", "Solomon", "Audrey", "Cristofer", "Ashlynn", "Dangelo", "Rhys",
    "Mariam", "Willie", "Lily", "Aspen", "Ali", "Kailey", "Brayan", "Alissa", "Crystal", "Sarah", "Marely",
    "Alyson", "Myah", "Alexzander", "Halle", "Kennedy", "Kasey", "Jaida", "Guillermo", "Alyssa", "Jonas",
    "Aiden", "Terrance", "Martha", "Gaven", "Gabriela", "Tucker", "Kaley", "Jadyn", "Linda", "Noelle",
    "Mckayla", "Jordyn", "Felicity", "Katie", "Kristina", "Jean", "India", "Martin", "Isabelle", "Shannon",
    "Lillian", "Jackson", "Stacy", "Lindsey", "Grayson", "Tabitha", "Bennett", "Alyvia", "Brandon", "Willie",
    "Kael", "Joy", "Brice", "Marianna", "Cameron", "Taliyah", "Emiliano", "Kelton", "Lisa", "Garrett",
    "Esmeralda", "Grace", "Pierre", "Margaret", "Rosemary", "Braedon", "Sebastian", "Leilani", "Ross",
    "Branson", "Rowan", "Haylie", "Abbey", "Brenna", "Selina", "Alijah", "Allen", "Rudy", "Eve", "Ireland",
    "Matteo", "Lizeth", "Marlon", "Jolie", "Avery", "Sanai", "Yandel", "Rodney", "Isabelle", "Delaney",
    "Skylar", "Niko", "Isabela", "Saul", "Jaylynn", "Iris", "Lesly", "Kaylen", "Todd", "Ignacio", "Natalee",
    "Destiney", "Ronnie", "Dixie", "Reilly", "Ralph", "Brenton", "Katherine", "Ibrahim", "Maggie", "Brennan",
    "Alondra", "Elizabeth", "Shaun", "Laylah", "Angelica", "Kendall", "Gage", "Zaniyah", "Fletcher", "Darion",
    "Denise", "Jaydan", "Adolfo", "Annie", "Lewis", "Arianna", "Mario", "Kenneth", "Valeria", "Ronin", "Joselyn",
    "Tyshawn", "Kiersten", "Shyla", "Ramon", "Isabella", "Avah", "Tristen", "Kylee", "Alisson", "Tanya", "Jonathon",
    "Harrison", "Ishaan", "Averie", "Lilia", "Ally", "Nikhil", "Mario", "Sydnee", "Tyree", "Griffin", "Mark",
    "Kiley", "Izayah", "Adolfo", "Nia", "Marlon", "Rubi", "Parker", "Easton", "Adalyn", "Julio", "Yareli",
    "Adriel", "Kale", "Kamren", "Jaidyn", "Tianna", "Miracle", "Roselyn", "Hope", "Sharon", "Amani", "Davis",
    "Kiersten", "Gabriel", "Turner", "Danny", "Angel", "Craig", "Zoie", "Melany", "Rylan", "Areli", "Brendon",
    "Cristofer", "Audrina", "Lina", "Shelby", "Madalynn", "Evelyn", "Anya", "Luciano", "Piper", "Kyler", "Riley",
    "Makaila", "Kayden", "Paige", "Raul", "Marlee", "Hanna", "Trey", "Hayley", "Kamren", "Dayana", "Colt", "Charles",
    "Kailyn", "Zackary", "Harper", "Macy", "Leon", "Raquel", "Evelyn", "Tiara", "Mckinley", "Alondra", "Yasmin",
    "Anabella", "Camryn", "Isabell", "Perla", "Graham", "Gael", "Jalen", "Terrance", "Cora", "Marlee", "Jadyn",
    "Jaylin", "Leland", "Ricardo", "Chanel", "Addyson", "Alayna", "Boston", "Lia", "Jane", "Clay", "Jordon",
    "Desmond", "Hezekiah", "Joseph", "Briana", "Jaylene", "Daniela", "Zechariah", "Brooks", "Josue", "Wyatt",
    "Khalil", "Salma", "Ivy", "Noe", "Janessa", "Marilyn", "Giovanny", "Payton", "Haylie", "Hanna", "Elian",
    "Campbell", "Quinton", "Laci", "Sandra", "Andreas", "Alice", "Clay", "Krish", "Jeremy", "Grace", "Isaac",
    "Colton", "Khloe", "Kendall", "Ivy", "Terrance", "Kade", "Madisyn", "Shelby", "Alyvia", "Camilla", "Lilly",
    "Maximus", "Alana", "Lucille", "Baron", "Violet", "Gary", "Braelyn", "Hudson", "Karma", "Micah", "Evangeline",
    "Sadie", "Yuliana", "Jerome", "Raelynn", "Isaac", "Nikolas", "Kristina", "Julien", "Davin", "Grant", "Brayan",
    "Elaina", "Braylen", "Nash", "Jillian", "Wesley", "Daniella", "Kieran", "Gaige", "Irene", "Jaylin", "Tatum",
    "Jovany", "Marques", "Aleah", "Manuel", "Alison", "Kasen", "Karter", "Anabella", "Jamarcus", "Morgan",
    "Christine", "Johnny", "Brandon", "Kobe", "Arthur", "Ryland", "Sabrina", "Aleah", "Brooklynn", "Manuel",
    "Charlotte", "Jacqueline", "Alexis", "Cash", "Jocelyn", "Rudy", "Eileen", "Desirae", "Esperanza", "Makai",
    "Davis", "Britney", "Leah", "Madeleine", "Gina", "George", "Darryl", "Dean", "Angel", "Tori", "Braxton", "Itzel",
    "Kareem", "Gael", "Giselle", "Xzavier", "Jovanny", "Lindsay", "Bo", "Silas", "Paola", "Mayra", "Diego", "Ashlee",
    "Abigayle", "Lilliana", "Daniel", "Layne", "Jeffery", "Pierre", "Nicolas", "Jaeden", "Jaylan", "Cassius", "Aden",
    "Terrence", "Rhett", "Damari", "Demarcus", "Makai", "Azaria", "Faith", "Khloe", "Itzel", "Gunner", "Giancarlo",
    "Marc", "Kaelyn", "Allisson", "Jessie", "Aliza", "Jade", "Lindsay", "Kristina", "Ainsley", "Brendan", "Leia",
    "Shyanne", "Brenden", "Alissa", "Helen", "Bernard", "Beckett", "Jessie", "Carlee", "Lesly", "Dominic", "Kai",
    "Gage", "Xander", "Kole", "Angelica", "Mauricio", "Elliot", "Lyric", "Elijah", "Rogelio", "Cassie", "Steven",
    "Bennett", "Ellie", "Ivy", "Sidney", "Kali", "Jordyn", "Nico", "Isabell", "Pranav", "Sarahi", "Justus", "Lexi",
    "Darius", "Sloane", "Lizbeth", "Landon", "Jocelyn", "Sasha", "Landyn", "Andres", "Tripp", "Corinne", "Jaylon",
    "Ellen", "Jaylene", "Rachael", "Luis", "Skylar", "Zachary", "Gwendolyn", "Camryn", "Finley", "Jan", "Lexi",
    "Marianna", "Mohamed", "Kailey", "Blaze", "Madilynn", "Robert", "Haley", "Matias", "Quinten", "Anabella",
    "Kimora", "Riya", "Davon", "Layton", "Leroy", "Rosa", "Greyson", "Tomas", "Emilio", "Belinda", "Keshawn",
    "Camille", "Logan", "Amira", "Ariana", "Skylar", "Julia", "Cullen", "Roman", "Wayne", "Marlee", "Talan", "Bryan",
    "Matthias", "June", "Skyler", "Fabian", "Bryanna", "Keyon", "Ellie", "Cheyenne", "Zachery", "Natalya", "Payton",
    "Tess", "Brendon", "Kianna", "Virginia", "Isabell", "Brenna", "Jaelyn", "Aryan", "Mathew", "Micah", "Xzavier",
    "Alma", "Landyn", "Bo", "Kenya", "Kaitlin", "Ethen", "Hugo", "Destiny", "Chase", "Eliza", "Warren", "America",
    "Julie", "Jamya", "Reese", "Wendy", "Korbin", "Alexander", "Bridget", "Carmen", "Stacy", "Kylan", "Ethan",
    "Seamus", "Ainsley", "Olivia", "Braxton", "Reese", "Grace", "Deanna", "Carlos", "Karen", "Rubi", "Brett",
    "Beckett", "Kobe", "Yandel", "Violet", "Dashawn", "Lauryn", "Orion", "Marianna", "Alaina", "Brynn", "Athena",
    "Cecilia", "Savanna", "Darrell", "Manuel", "Dylan", "Maci", "Dawson", "Karissa", "Quinten", "Nayeli", "Cortez",
    "Keith", "Tristin", "Justine", "Rhianna", "Annabella", "Giovanna", "Annabella", "Tristian", "Gaige", "Leticia",
    "Mikaela", "Lillie", "Rylan", "Bronson", "Leandro", "Itzel", "Axel", "Nolan", "Zaria", "Cassie", "Alayna",
    "Cassandra", "Jovanny", "Kash", "Emery", "Jade", "Elijah", "Abagail", "Dallas", "Skylar", "Kamari", "Carissa",
    "Arthur", "Maliyah", "Aubree", "Vaughn", "Niko", "Dulce", "Selah", "Bailey", "Camden", "Sofia", "Darrell",
    "Adison", "Gia", "Jayvion", "Julie", "Arianna", "Paris", "Vincent", "Lainey", "Seamus", "Christine",
    "Cristopher", "Gavin", "Kenyon", "Cristina", "Joselyn", "Brynn", "Meghan", "Joy", "Lindsey", "Jazmine",
    "Ashlee", "Orlando", "Donovan", "Heath", "Jazlyn", "Macy", "Tyrell", "Jane", "Dashawn", "Jaiden", "Tobias",
    "Lance", "Kaylynn", "Armando", "Marisol", "Anastasia", "Jimmy", "Messiah", "Arianna", "Kael", "Amare", "Yadiel",
    "Willie", "Ashlee", "Billy", "Clayton", "Amaya", "Jensen", "Junior", "Easton", "Quinn", "Leo", "Adeline", "Jacob",
    "Cael", "Carolina", "Nyasia", "Keenan", "Phillip", "Alfred", "Jesus", "Maci", "Dale", "Micheal", "Keaton",
    "Alena", "Miguel", "Mckenna", "Diya", "Lewis", "Malcolm", "Ashley", "Kara", "Karli", "Malaki", "Uriel",
    "Carolyn", "Payton", "Lennon", "Vivian", "Grayson", "Blake", "Moses", "Mylee", "Braydon", "Kaylynn", "Rylie",
    "Avery", "Tessa", "Nathaly", "Evan", "Skylar", "Jayden", "Aubrie", "Elijah", "Kassidy", "Kristin", "Charlee",
    "Marquis", "June", "Madison", "Felicity", "Ernest", "Davion", "Nico", "Deborah", "Leah", "Nickolas", "Emelia",
    "Giovani", "Arthur", "Sergio", "Mitchell", "Nicolas", "Salvatore", "Abby", "Immanuel", "Jeramiah", "Shea",
    "Jacob", "Isaias", "Avery", "Naima", "Marely", "Zaid", "Charlotte", "Jacob", "Franklin", "Quentin", "Mathew",
    "Andrea", "Kian", "Lana", "Abbey", "Liberty", "Ace", "Leah", "Melody", "Elaine", "Avery", "Alisa", "Ashanti",
    "Jaylen", "Michaela", "Melvin", "Iliana", "Ronnie", "Zachery", "Jaidyn", "Madelynn", "Maryjane", "Giada", "Kylie",
    "Aryanna", "Cailyn", "Zachery", "Annalise", "Quinn", "Alec", "Braylen", "Raiden", "Cole", "Elsie", "Aydan",
    "Gracie", "Corinne", "Saul", "Antony", "Muhammad", "Weston", "Bryanna", "Kody", "Skylar", "Leslie", "Luna",
    "Gretchen", "London", "Hadassah", "Alaina", "Bobby", "Jasiah", "Xander", "Sydnee", "Landin", "Ty", "Fletcher",
    "Henry", "Janiya", "Max", "Danika", "Kylie", "Bianca", "Heidi", "Dalia", "Lexie", "Cecelia", "Jan", "Freddy",
    "Hector", "Aaden", "Jaron", "Destiney", "Bryan", "Ramiro", "Lorelei", "Rose", "Madison", "Kathleen", "Jude",
    "Garrett", "Kamden", "Clayton", "Bradyn", "Aniya", "Haven", "Lucas", "Emilie", "Jacey", "Micaela", "Raegan",
    "Lillianna", "Izabella", "Justice", "Kaitlyn", "Anabelle", "Jerry", "Cassius", "Benjamin", "Mireya", "Ryann",
    "Liana", "Darren", "Madilyn", "Blaine", "Tania", "Elliott", "Amaya", "Brennan", "Joy", "Quinton", "Soren",
    "Jeremiah", "Rylie", "Frederick", "Preston", "Sandra", "Savanah", "Cesar", "Kaylin", "Alejandro", "Jaylah",
    "Amiya", "Kaiya", "Hillary", "Thomas", "Carlos", "Kiara", "Immanuel", "Kolton", "Toby", "Giada", "Elizabeth",
    "Maliyah", "Zara", "Rodolfo", "Kristen", "Piper", "Allen", "Keira", "Amaya", "Briley", "Santiago", "Lorelai",
    "Abigayle", "Sanai", "Eileen", "Evie", "Jesse", "Janiah", "Jenny", "Valentino", "Jamir", "Jefferson", "Jayla",
    "Hayden", "Israel", "Wyatt", "Terrance", "Emilie", "Mackenzie", "Quincy", "Gavyn", "Cornelius", "Roland",
    "Abril", "Chaz", "Skyler", "Athena", "Kody", "Kamari", "Kaitlin", "Yandel", "Valentin", "Martin", "Aaron",
    "Camilla", "Dominik", "Alan", "Leon", "Cole", "Ernesto", "Zariah", "Griffin", "Sheldon", "Allie", "Ashly",
    "Justice", "Lilah", "Paisley", "Gabriela", "Arthur", "Ruben", "Yusuf", "Luis", "London", "Martin", "Kolby",
    "Clare", "Anahi", "Tiffany", "Isabel", "Killian", "Anabel", "Marley", "Harrison", "Marely", "Clayton", "Terrell",
    "Litzy", "Sawyer", "Lilia", "Maia", "Solomon", "Jamya", "Hannah", "Payten", "Skyler", "Mohamed", "Beau",
    "Brayan", "Shyanne", "Xavier", "Helen", "Aidan", "Kaitlyn", "Luka", "Rey", "Abril", "Bailee", "Reuben",
    "Giovanni", "Jacey", "Harley"
  )

  val lastnames = Seq(
    "Harding", "Vega", "Marshall", "Hester", "Hester", "Delacruz", "Serrano", "Lang", "Maxwell", "Wang", "Cooper",
    "Zuniga", "Arellano", "Mendoza", "Carroll", "Baird", "Soto", "Newton", "Middleton", "Arias", "Copeland",
    "Morrison", "Pineda", "Goodman", "Campbell", "Pena", "Gonzalez", "Spears", "Lawson", "Doyle", "Moran", "Andersen",
    "Yates", "Webb", "Gentry", "Edwards", "Nunez", "Schroeder", "Guerrero", "Galvan", "Nguyen", "Mcneil", "Gilbert",
    "Lamb", "Sheppard", "Marshall", "Day", "Bond", "Wood", "Garner", "Armstrong", "Dunlap", "Collier", "Yang",
    "Macdonald", "Andersen", "Clark", "Prince", "Sullivan", "Day", "Frank", "Landry", "Brooks", "Barry", "Kelly",
    "Roy", "Mullins", "Suarez", "Ho", "White", "Tate", "Rasmussen", "Cochran", "Davidson", "Parrish", "Gallagher",
    "Flowers", "Clayton", "Patrick", "Donovan", "Waters", "Ramirez", "Ramsey", "Dunn", "Warren", "Blankenship",
    "Schroeder", "Peck", "Doyle", "Horne", "Haley", "Pierce", "Chandler", "Russo", "Conner", "Mccarty", "Frye",
    "Christensen", "Maddox", "Christensen", "Cantu", "Barry", "Wiggins", "Dean", "Baxter", "Mcpherson", "Black",
    "Frost", "Waller", "Boyer", "Galvan", "Cobb", "Andrade", "Cobb", "Kent", "Wilkerson", "Spencer", "Ruiz", "Burns",
    "Bass", "Branch", "Travis", "Lewis", "Landry", "Kent", "Blake", "Norris", "Humphrey", "Garrett", "Park", "Buchanan",
    "Faulkner", "Benitez", "Leonard", "Chung", "May", "Chavez", "Hines", "Benjamin", "Melendez", "Atkinson",
    "Holloway", "Haley", "Gaines", "Reyes", "Stafford", "Wu", "Shea", "Henry", "Hale", "Kramer", "Wagner", "Deleon",
    "Ward", "Mata", "Zavala", "Mcmahon", "Payne", "Higgins", "Curry", "Roach", "Guerra", "Cooke", "Duran", "Conner",
    "Meyer", "Reyes", "Malone", "Pham", "Gilbert", "Moss", "Moore", "Zhang", "Rosales", "Love", "Manning", "Crane",
    "Reid", "Schaefer", "Ortiz", "Ford", "Joyce", "Elliott", "Ewing", "Mayer", "Gallagher", "Marshall", "Woodward",
    "Hunt", "Bartlett", "Faulkner", "Reese", "Beck", "Keith", "Lutz", "Edwards", "Pitts", "Mcclure", "Mcconnell",
    "Knox", "Rasmussen", "Li", "Alvarado", "Mendoza", "Jimenez", "Zamora", "Marks", "Lowery", "Jimenez", "Huang",
    "Duarte", "Velasquez", "Fitzpatrick", "Gray", "Harrell", "Gates", "Wu", "Page", "Powell", "Ray", "Rhodes",
    "Kaiser", "Pineda", "Preston", "Pitts", "Archer", "Peck", "Chambers", "Booth", "York", "Zamora", "Maldonado",
    "Dickerson", "Mcmillan", "Tucker", "Patterson", "Daniels", "Dougherty", "Hendrix", "Hood", "Martinez", "Knox",
    "Browning", "Adkins", "Oconnor", "Duran", "Neal", "Andrews", "Mahoney", "Reeves", "George", "Gould", "Singleton",
    "Mosley", "Morton", "Christensen", "Mckay", "Becker", "Carr", "Griffin", "Snow", "Barrett", "Michael", "Bond",
    "Butler", "Vega", "Ritter", "Ewing", "Baird", "Howell", "Duarte", "Valentine", "Rodgers", "Ayala", "Randolph",
    "Blake", "Kelley", "Castillo", "Tate", "Clarke", "Burnett", "Fuentes", "Potts", "Walter", "Frank", "Bruce",
    "Craig", "Hayes", "Zuniga", "Garner", "Salinas", "Marquez", "Richmond", "Livingston", "Clarke", "Allison", "Howe",
    "Goodwin", "Washington", "Rice", "Escobar", "Kelley", "Yang", "Vance", "Carter", "Davies", "Dickson", "Ray",
    "Hendricks", "Madden", "Butler", "Lambert", "Morrison", "Berg", "Fisher", "Larsen", "Ford", "Arroyo", "Nielsen",
    "Cook", "Johnson", "Kline", "Reeves", "Chen", "Guerrero", "Meyers", "Mcdowell", "Mayo", "Morrison", "Vance",
    "Cooper", "Mack", "Benjamin", "Avila", "Browning", "Chan", "Holland", "Schmitt", "Logan", "Wu", "Bennett",
    "Edwards", "Carrillo", "Paul", "Snyder", "Johnston", "Koch", "Robertson", "Fitzgerald", "Benitez", "Sanchez",
    "Hoover", "Black", "Roberts", "Patterson", "Wheeler", "Terry", "Hall", "Liu", "Atkins", "Petersen", "Day",
    "Hernandez", "Allison", "Boyer", "Cannon", "Dougherty", "Flores", "Mercado", "Barton", "Frank", "Vega", "Howell",
    "Barton", "Lin", "Barker", "Kim", "Daniels", "Hogan", "Huff", "Cortez", "Castro", "Cantu", "Velez", "Hendricks",
    "Kaufman", "Brock", "Koch", "Cruz", "Dickerson", "Salinas", "Cooke", "Sellers", "Manning", "Williams", "Jenkins",
    "Chambers", "George", "Peck", "Porter", "Hodge", "Frank", "Bautista", "Kennedy", "Oneal", "Rocha", "Harrison",
    "Moon", "Villegas", "Massey", "Lam", "Moody", "Terrell", "Mayer", "Nichols", "Kaufman", "Meza", "Lucas", "Kent",
    "Grimes", "Collier", "Odonnell", "French", "Landry", "Hughes", "Stein", "Roberson", "Powell", "Jimenez", "Chase",
    "Russell", "Summers", "Chapman", "Kline", "Berger", "Martin", "Blake", "Humphrey", "Nguyen", "Mcfarland", "Hill",
    "Roach", "Garrett", "Allison", "Maynard", "Haney", "Burnett", "Le", "Frey", "Pierce", "Mays", "Castillo", "Skinner",
    "Barrera", "Alvarez", "Booth", "Flowers", "Jenkins", "Bonilla", "Hood", "Gray", "Yang", "Frank", "Mcneil", "Frost",
    "Dyer", "Morris", "Brewer", "Hancock", "Moon", "Hebert", "Walsh", "Pope", "Greer", "Hall", "Howard", "Shannon",
    "Torres", "Buck", "Browning", "Conrad", "Frey", "Hicks", "Perkins", "Baldwin", "Galloway", "Mcknight", "Cuevas",
    "Stokes", "Garner", "Stafford", "Monroe", "Padilla", "Rodriguez", "Flynn", "Avery", "Wang", "Koch", "Wise",
    "Sanders", "Grimes", "Cole", "Hardy", "Norton", "Cole", "Pitts", "Oneal", "Burnett", "Fuentes", "Campbell",
    "Becker", "Mcgee", "Wall", "Dickson", "Owens", "Ali", "Pierce", "Keith", "Sexton", "Abbott", "Schneider", "Proctor",
    "Hawkins", "Ward", "Garrison", "Vincent", "Deleon", "Burch", "Morrison", "Hubbard", "Mccarty", "Conley", "Sullivan",
    "Roy", "Wood", "Duncan", "Bonilla", "Baker", "Waters", "Obrien", "Buck", "Juarez", "Bowman", "Wang", "Jacobson",
    "Dalton", "Villarreal", "Acevedo", "Roth", "Duran", "Park", "Stevenson", "Bennett", "Mercer", "Irwin", "Lam",
    "Padilla", "Ochoa", "Morrison", "Hickman", "Best", "Griffith", "Sweeney", "Vance", "Hanna", "Collins", "Krause",
    "Cross", "Washington", "Hebert", "Blake", "Gallegos", "Conley", "Padilla", "Chen", "Rocha", "Bruce", "Dickerson",
    "Rodgers", "Dudley", "Jimenez", "Humphrey", "Davis", "Flores", "Byrd", "Guerra", "Gilmore", "Cross", "Herman",
    "Underwood", "Porter", "Ali", "George", "Keller", "Conrad", "Dyer", "Lee", "Rhodes", "Mendoza", "Hodge", "Munoz",
    "Mccall", "Pace", "Gregory", "Alvarado", "Mack", "Duffy", "Carter", "Kennedy", "Frey", "Richard", "Farmer",
    "Bennett", "Ross", "Mcfarland", "Horne", "Cunningham", "Branch", "Best", "Cox", "Oneal", "Miller", "Deleon",
    "Ayers", "Serrano", "Moody", "Walters", "Brandt", "Yu", "Riggs", "Hendricks", "Richards", "Hutchinson", "Olsen",
    "Crosby", "Oconnor", "Hicks", "Bond", "Olsen", "Valdez", "Hoffman", "Poole", "Chang", "Avery", "Solis",
    "Gallegos", "Solis", "Sanchez", "Wallace", "Todd", "Potter", "Reed", "Myers", "Stanley", "Vaughan", "Singleton",
    "Mccarthy", "Glenn", "David", "Raymond", "Maxwell", "Peterson", "Ewing", "Montes", "Fuller", "Hodges", "Vaughn",
    "Shaw", "Holmes", "Hodge", "Joseph", "Glass", "Zavala", "Jefferson", "Garrison", "Stone", "Whitaker", "Baxter",
    "Olsen", "Guerrero", "Powell", "Ashley", "Middleton", "Jensen", "Crosby", "Farrell", "Bradford", "Nixon", "Ibarra",
    "Thornton", "Dickson", "Willis", "Faulkner", "Chavez", "Barry", "French", "Galvan", "Thornton", "Collier",
    "Gilmore", "Riggs", "Perry", "Clayton", "Donovan", "Blair", "Shea", "Scott", "Castro", "Knox", "Reyes", "Stout",
    "Brewer", "Fry", "Sanders", "Aguirre", "White", "Vazquez", "Garner", "Mosley", "Howe", "Cole", "Ray", "Franklin",
    "Fischer", "Blevins", "Chase", "Tyler", "Crosby", "Parsons", "Rosales", "Flowers", "Nolan", "Lin", "Trujillo",
    "Ayala", "Morales", "Velazquez", "Heath", "Arnold", "Brock", "Liu", "Wells", "Marsh", "Webb", "Pugh", "Irwin",
    "Barton", "Davis", "Johnston", "Atkins", "Finley", "Romero", "Bowers", "Simpson", "Craig", "Mckinney", "Sampson",
    "Mclaughlin", "Sullivan", "Cummings", "Villegas", "Brooks", "Rodgers", "Davenport", "Glass", "Chen", "Lin", "Bass",
    "Stein", "Carney", "Baird", "Stevenson", "Gillespie", "Chavez", "Foley", "Daugherty", "Terry", "Vaughan", "Burns",
    "Reeves", "Santana", "Grimes", "Marquez", "Rodriguez", "Butler", "Liu", "Atkinson", "Perry", "Francis", "Pham",
    "Stanton", "Monroe", "Ballard", "Stanley", "Galvan", "Hoover", "Kane", "Khan", "Hopkins", "Harrison", "Briggs",
    "Berry", "Gill", "Copeland", "House", "Donaldson", "Parker", "Burgess", "Walsh", "Garza", "Campbell", "Matthews",
    "Miranda", "Joyce", "Oneill", "Sanford", "Mcmillan", "Saunders", "Church", "Lindsey", "Whitney", "Fry", "Sellers",
    "Dickson", "Frye", "Fuller", "Gibbs", "Mullins", "Garrett", "Rivas", "Montes", "Krueger", "Ellison", "Avery",
    "Bullock", "Andrews", "Thompson", "Callahan", "Willis", "Juarez", "Solis", "Erickson", "Wade", "Miller", "Pollard",
    "Fry", "Villarreal", "Kemp", "Jarvis", "Mcconnell", "Elliott", "Chavez", "Barrera", "Mathis", "Haas", "Chang",
    "Cole", "Cohen", "Miles", "Parrish", "Brandt", "Hall", "Strickland", "Goodman", "Orr", "Moody", "Garcia", "Hartman",
    "Keith", "Benson", "Obrien", "Wilson", "Mullen", "Gregory", "Oneill", "Hobbs", "Carey", "Vang", "Livingston",
    "Avila", "Hodges", "Pena", "Webb", "Ali", "Parks", "Ellison", "Baxter", "Trevino", "Norris", "Shelton", "Miles",
    "Sherman", "Boyle", "Potter", "Rowe", "Wiggins", "Baker", "Kim", "Kent", "Mays", "Berry", "Hines", "Dalton",
    "Bartlett", "Buck", "Mcgrath", "Roman", "Rogers", "Rodgers", "Richard", "Dixon", "Olson", "Zuniga", "Carlson",
    "Barton", "Shepard", "Alvarado", "Sheppard", "Lamb", "Marsh", "Deleon", "Valencia", "Savage", "Rivas", "Jacobs",
    "Roman", "Rivers", "Diaz", "Greer", "Flores", "Roberts", "King", "Mathis", "Lozano", "Bond", "Macdonald", "Stone",
    "Stafford", "Dougherty", "Shields", "Perkins", "Riddle", "Friedman", "Webb", "Castaneda", "Good", "Rangel",
    "Yoder", "Park", "Aguirre", "Cook", "Chan", "Ashley", "Lewis", "Key", "Fischer", "Marks", "Reynolds", "Pollard",
    "Dudley", "Fox", "Logan", "Mclaughlin", "Brandt", "Parker", "Summers", "Bush", "Mcmillan", "McmahonWerner",
    "Reilly", "Stephens", "Stevenson", "Werner", "Wang", "Gallegos", "Prince", "Caldwell", "Gould", "Nelson", "Small",
    "Kemp", "Chandler", "Schneider", "Travis", "Avery", "Nelson", "Swanson", "Poole", "Porter", "Pitts", "Dyer",
    "Garcia", "Goodman", "Luna", "Oneal", "French", "Flores", "Christian", "Brewer", "Brady"
  )

}