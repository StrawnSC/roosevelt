import sqlite3 
import unittest
import random

class Connector:
	DB_NAME = None
	DB_CONNECTION = None

	@classmethod
	def is_connected(cls):
		return cls.DB_CONNECTION is not None

	# NTH don't do this unless the connection is None
	@classmethod
	def connect(cls, db_name:str):
		cls.DB_NAME = db_name
		cls.DB_CONNECTION = sqlite3.connect(db_name)

	@classmethod
	def disconnnect(cls):
		assert cls.is_connected()
		cls.DB_CONNECTION.close()
		cls.DB_CONNECTION = None
		cls.DB_NAME = None

	@classmethod
	def execute(cls, sql:str):
		assert cls.is_connected
		cursor = cls.DB_CONNECTION.cursor()
		cursor.execute(sql)
		cls.DB_CONNECTION.commit()
		return cursor 
		
class Field:
	@classmethod 
	def sqlite_type_name(cls) -> str:
		raise NotImplementedError

	def column_declaration(self, field_name) -> str:
		return f"{field_name} {self.sqlite_type_name()}"

	# used for foreignkey constraint -- must be put in after all the columns are declared
	def get_post_declaration_constraint(self, field_name) -> str:
		return None

	@classmethod 
	def sqlite_type_name(cls) -> str:
		raise NotImplementedError

	# convert python value -> sqlite value 
	@classmethod
	def convert(cls, py_val):
		return py_val 

	# convert python value -> sqlite value 
	@classmethod
	def unconvert(cls, sqlite_val):
		return sqlite_val 


class BooleanField(Field):
	@classmethod 
	def sqlite_type_name(cls) -> str:
		return "INTEGER" 

	@classmethod
	def convert(cls, py_val):
		return 1 if py_val else 0 

	@classmethod
	def unconvert(cls, sqlite_val):
		return bool(sqlite_val) 

class IntField(Field):
	@classmethod 
	def sqlite_type_name(cls) -> str:
		return "INTEGER"

class FloatField(Field):
	@classmethod 
	def sqlite_type_name(cls) -> str:
		return "REAL"

class StringField(Field):
	@classmethod 
	def sqlite_type_name(cls) -> str:
		return "TEXT"

	# convert python value -> sqlite value 
	@classmethod
	def convert(cls, py_val):
		return f"'{py_val}'" 

class ForeignKeyField(Field):
	@classmethod 
	def sqlite_type_name(cls) -> str:
		return "INTEGER"

	def __init__(self, other_cls):
		self.other_cls = other_cls

	def get_post_declaration_constraint(self, field_name) -> str:
		return f"FOREIGN KEY({field_name}) REFERENCES {self.other_cls._table_name()}(rowid)"

	# TODO -- this assumes that the other val you're referencing was already saved in the DB
	# convert python value -> sqlite value 
	@classmethod
	def convert(cls, py_val):
		return py_val._id 

# TODO make sure that the order of vars(cls).items() is deterministic/fixed
# NTH define the equals method 
class Model:
	@classmethod 
	def print_fields(cls):
		for s in vars(cls):
			if not s.startswith("_"):
				print(s)

	# NTH allow passing in data values as kwargs or positional args 
	def __init__(self):
		self._id = None 
			
	
	# NTH change this to lazily evaluate foreign key objects -- perhaps using @property? 
	# TODO make sure this loads types in the right format -- do we need to unconvert?
	@classmethod
	def init_from_db_tuple(cls, tup):
		obj = cls()
		rowid, *vals = tup 
		idx = 0
		for field_name, field in vars(cls).items():
			if not field_name.startswith("_"):
				if isinstance(field, ForeignKeyField):
					other_id = vals[idx] 
					loaded = cls.load_fk_field(field, other_id)
					setattr(obj, field_name, loaded)
				else:
					setattr(obj, field_name, field.unconvert(vals[idx]))
				idx += 1
		obj._id = rowid
		return obj

	@classmethod 
	def load_fk_field(cls, field: ForeignKeyField, other_id):
		cursor = Connector.execute(f"SELECT rowid, * FROM {field.other_cls._table_name()} WHERE rowid={other_id} LIMIT 1")
		tup = cursor.fetchone()
		return field.other_cls.init_from_db_tuple(tup)

	@classmethod
	def _table_name(cls):
		return cls.__name__

	# NTH cache this result 
	@classmethod
	def _table_exists(cls) -> bool:
		sql = f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{cls._table_name()}'"
		c = Connector.execute(sql)
		return c.fetchone()[0]==1

	# NTH raise an exception on bad column names 
	@classmethod
	def _create_table(cls):
		# note that sqlite creates a primary key called "rowid" by default 
		sql = list(f"CREATE TABLE {cls._table_name()} (")
		columns = []
		for field_name, field in vars(cls).items():
			if isinstance(field, ForeignKeyField): # ensure that the foreignkey table exists too -- this will loop forever if there's a cycle in the schemas
				field.other_cls._ensure_table_exists()
			if not field_name.startswith("_"):
				columns.append(field.column_declaration(field_name))
		
		for field_name, field in vars(cls).items():
			if not field_name.startswith("_"):
				constr = field.get_post_declaration_constraint(field_name)
				if constr is not None:
					columns.append(constr)

		for i, c in enumerate(columns):
			sql.append(c)
			if i != len(columns) - 1:
				sql.append(", ")

		sql.append(")")
		Connector.execute("".join(sql))

	@classmethod
	def _ensure_table_exists(cls):
		if not cls._table_exists():
			cls._create_table()

	def save(self):
		self._ensure_table_exists()
		if self._id is None: 
			sql = list(f"INSERT INTO {self._table_name()} VALUES (")
			columns = []
			for field_name, field in vars(type(self)).items():
				if not field_name.startswith("_"):
					columns.append(field.convert(vars(self)[field_name]))  
			
			for i, c in enumerate(columns):
				sql.append(str(c))
				if i != len(columns) - 1:
					sql.append(", ")

			sql.append(")")
			cmd = "".join(sql)
			cursor = Connector.execute(cmd)
			self._id = cursor.lastrowid
		else: 
			sql = list(f"UPDATE {self._table_name()} SET ")

			columns = []
			for field_name, field in vars(type(self)).items():
				if not field_name.startswith("_"):
					columns.append(f"{field_name}={field.convert(vars(self)[field_name])}")  

			for i, c in enumerate(columns):
				sql.append(c)
				if i != len(columns) - 1:
					sql.append(", ")

			sql.append(f" WHERE rowid={self._id}")
			cmd = "".join(sql)
			cursor = Connector.execute(cmd)

			

	def delete(self):
		self._ensure_table_exists()
		sql = f"DELETE FROM {self._table_name()} WHERE rowid={self._id}"
		cursor = Connector.execute(sql)

	@classmethod
	def objects(cls, **kwargs):
		return QuerySet(cls, **kwargs) 

# Note: eagerly evaluates foreign keys (not super performant)
class QuerySet:
	class Iterator:
		def __init__(self, qs):
			self.idx = 0
			self.qs = qs

		def __next__(self):
			if self.idx >= len(self.qs.items):
				raise StopIteration
			else:
				elem = self.qs.model_cls.init_from_db_tuple(self.qs.items[self.idx])
				self.idx += 1
				return elem

	def __iter__(self):
		return self.Iterator(self) 

	def __getitem__(self, key):
		return self.model_cls.init_from_db_tuple(self.items[key]) 

	def __init__(self, model_cls, **kwargs):
		self.filter_kwargs = kwargs 
		self.model_cls = model_cls
		self.query_result = Connector.execute(self.get_query_string())
		self.items = self.query_result.fetchall()

	# NTH cache this  
	def get_query_string(self) -> str:
		sql = list(f"SELECT rowid, * FROM {self.model_cls._table_name()}")
		if self.filter_kwargs:
			sql.append(" WHERE ")

		i = 0
		for k, v in self.filter_kwargs.items():
			converted_val = vars(self.model_cls)[k].convert(v)
			sql.append(f"{k}={converted_val} ")
			if i != len(self.filter_kwargs.items()) - 1:
				sql.append("AND ")
			i += 1 

		return "".join(sql)

	def count(self):
		return len(self.items)
	

# Test Cases

class AbstractDBTest(unittest.TestCase):
	def setUp(self):
		Connector.connect(":memory:")

	def tearDown(self):
		Connector.disconnnect()

class TestCreate(AbstractDBTest):
	def test_basic_model_saving(self):
		class Student(Model):
			eid = IntField()
			graduating_year = StringField()
			name = StringField()
			gpa = FloatField()

		silas = Student()
		silas.eid = 3434
		silas.graduating_year = "2021"
		silas.name = "Silas Strawn"
		silas.gpa = 3.86

		silas.save()

		rows = [row for row in Connector.execute("SELECT * FROM Student")]

		self.assertEqual(len(rows), 1)
		self.assertEqual(rows[0], (3434, '2021', 'Silas Strawn', 3.86))

	def test_multiple_instance_saving(self):
		class Student(Model):
			eid = IntField()
			graduating_year = StringField()
			name = StringField()
			gpa = FloatField()
			is_graduating = BooleanField()

		num_students = 12

		students = [Student() for i in range(num_students)]

		for s in students:
			s.eid = random.randint(0, 1000)
			s.graduating_year = str(random.randint(2022, 2025))
			s.name = f"John or Jane Doe #{s.eid}"
			s.gpa = random.random() * 4
			s.is_graduating = s.graduating_year == 2022
			s.save()

		rows = [row for row in Connector.execute("SELECT * FROM Student")]

		self.assertEqual(len(rows), num_students)

	def test_saving_with_fk(self):
		class Course(Model):
			dept = StringField()
			name = StringField()

		class Student(Model):
			eid = StringField()

		class EnrollmentRecord(Model):
			course = ForeignKeyField(Course)
			enrolled_student = ForeignKeyField(Student)

		chem = Course()
		chem.name = "General Chemistry"
		chem.dept = "Science"
		chem.save()

		stud = Student()
		stud.eid = "1234"
		stud.save()

		enrollment = EnrollmentRecord()
		enrollment.course = chem
		enrollment.enrolled_student = stud 

		enrollment.save()

class TestRetrieve(AbstractDBTest):
	def test_basic_retrieve(self):
		class Student(Model):
			eid = IntField()
			graduating_year = StringField()
			name = StringField()
			gpa = FloatField()
			is_graduating = BooleanField()

		num_students = 12

		students = [Student() for i in range(num_students)]
		s_map = {}

		for s in students:
			s.eid = random.randint(0, 1000)
			s.graduating_year = str(random.randint(2022, 2025))
			s.name = f"John or Jane Doe #{s.eid}"
			s.gpa = random.random() * 4
			s.is_graduating = s.graduating_year == 2022
			s.save()

			s_map[s._id] = s

		students_from_db = Student.objects()
		self.assertEqual(students_from_db.count(), num_students)

		for s in students_from_db:
			original = s_map[s._id]
			self.assertEqual(s.eid, original.eid)
			self.assertEqual(s.graduating_year, original.graduating_year)
			self.assertEqual(s.name, original.name)
			self.assertEqual(s.gpa, original.gpa)
			self.assertEqual(s.is_graduating, original.is_graduating)

	def test_basic_filtering_retrieve(self):
		class Student(Model):
			eid = IntField()
			graduating_year = StringField()
			name = StringField()
			gpa = FloatField()
			is_graduating = BooleanField()

		num_students = 12

		students = [Student() for i in range(num_students)]
		s_map = dict()
		fish = set()
		non_fish = set()


		for s in students:
			s.eid = random.randint(0, 1000)
			s.graduating_year = str(random.randint(2022, 2025))
			s.name = f"John or Jane Doe #{s.eid}"
			s.gpa = random.random() * 4
			s.is_graduating = s.graduating_year == 2022
			s.save()

			s_map[s._id] = s
			if s.graduating_year == str(2025):
				fish.add(s)
			else:
				non_fish.add(s)

		students_from_db = Student.objects(graduating_year="2025")
		self.assertEqual(students_from_db.count(), len(fish))

		for s in students_from_db:
			original = s_map[s._id]
			self.assertEqual(s.graduating_year, "2025")
			self.assertEqual(s.eid, original.eid)
			self.assertEqual(s.graduating_year, original.graduating_year)
			self.assertEqual(s.name, original.name)
			self.assertEqual(s.gpa, original.gpa)
			self.assertEqual(s.is_graduating, original.is_graduating)

	# necessary to test this because BooleanFields are represented differently in the DB (as ints)
	def test_boolean_retrieval(self):
		class BoolSet(Model):
			first = BooleanField()
			second = BooleanField()
		
		bs1 = BoolSet(); bs1.first=True; bs1.second=True; bs1.save()
		bs2 = BoolSet(); bs2.first=True; bs2.second=False; bs2.save()
		bs3 = BoolSet(); bs3.first=False; bs3.second=True; bs3.save()
		bs4 = BoolSet(); bs4.first=False; bs4.second=False; bs4.save()

		obj_map = {bs._id: bs for bs in [bs1, bs2, bs3, bs4]}

		for bs in BoolSet.objects():
			self.assertEqual(bs.first, obj_map[bs._id].first)
			self.assertEqual(bs.second, obj_map[bs._id].second)

	def test_filter_on_multiple_columns(self):
		class Coord3D(Model):
			x = IntField()
			y = IntField()
			z = IntField()

		p1 = Coord3D(); p1.x=1; p1.y=2; p1.z=3; p1.save()
		p2 = Coord3D(); p2.x=1; p2.y=3; p2.z=2; p2.save()
		p3 = Coord3D(); p3.x=3; p3.y=2; p3.z=3; p3.save()

		qs = Coord3D.objects(x=1, y=2, z=3)
		
		self.assertEqual(qs.count(), 1)
		for p in qs:
			self.assertEqual(p._id, p1._id)
			self.assertEqual(p.x, p1.x)
			self.assertEqual(p.y, p1.y)

	def test_retreive_with_fk(self):
		class Course(Model):
			dept = StringField()
			name = StringField()

		class Student(Model):
			eid = StringField()

		class EnrollmentRecord(Model):
			course = ForeignKeyField(Course)
			enrolled_student = ForeignKeyField(Student)

		chem = Course()
		chem.name = "General Chemistry"
		chem.dept = "Science"
		chem.save()

		stud = Student()
		stud.eid = "1234"
		stud.save()

		enrollment = EnrollmentRecord()
		enrollment.course = chem
		enrollment.enrolled_student = stud 

		enrollment.save()

		qs = EnrollmentRecord.objects()
		
		self.assertEqual(qs.count(), 1)

		for er in qs:
			self.assertEqual(er.course._id, chem._id)
			self.assertEqual(er.course.name, chem.name)
			self.assertEqual(er.course.dept, chem.dept)

			self.assertEqual(er.enrolled_student._id, stud._id)
			self.assertEqual(er.enrolled_student.eid, stud.eid)

class TestUpdate(AbstractDBTest):
	def test_basic_update(self):
		class Coord3D(Model):
			x = IntField()
			y = IntField()
			z = IntField()

		p1 = Coord3D(); p1.x=1; p1.y=2; p1.z=3; p1.save()

		loaded = Coord3D.objects()[0]
		self.assertEqual(loaded.x, 1)
		self.assertEqual(loaded.y, 2)
		self.assertEqual(loaded.z, 3)

		p1.x=1; p1.y=3; p1.z=2; p1.save()

		loaded = Coord3D.objects()[0]
		self.assertEqual(loaded.x, 1)
		self.assertEqual(loaded.y, 3)
		self.assertEqual(loaded.z, 2)

		p1.x=3; p1.y=2; p1.z=3; p1.save()

		loaded = Coord3D.objects()[0]
		self.assertEqual(loaded.x, 3)
		self.assertEqual(loaded.y, 2)
		self.assertEqual(loaded.z, 3)

	def test_update_only_changes_one(self):
		class Coord3D(Model):
			x = IntField()
			y = IntField()
			z = IntField()
			name = StringField()

		p1 = Coord3D(); p1.name="p1"; p1.x=1; p1.y=2; p1.z=3; p1.save()
		p2 = Coord3D(); p2.name="p2"; p2.x=1; p2.y=3; p2.z=2; p2.save()
		p3 = Coord3D(); p3.name="p3"; p3.x=3; p3.y=2; p3.z=3; p3.save()

		def test_others_unchanged():
			loaded = Coord3D.objects(name="p2")[0]
			self.assertEqual(loaded.x, 1)
			self.assertEqual(loaded.y, 3)
			self.assertEqual(loaded.z, 2)
			loaded = Coord3D.objects(name="p3")[0]
			self.assertEqual(loaded.x, 3)
			self.assertEqual(loaded.y, 2)
			self.assertEqual(loaded.z, 3)

		loaded = Coord3D.objects(name="p1")[0]
		self.assertEqual(loaded.x, 1)
		self.assertEqual(loaded.y, 2)
		self.assertEqual(loaded.z, 3)
		test_others_unchanged()

		p1.x=1; p1.y=3; p1.z=2; p1.save()

		loaded = Coord3D.objects(name="p1")[0]
		self.assertEqual(loaded.x, 1)
		self.assertEqual(loaded.y, 3)
		self.assertEqual(loaded.z, 2)
		test_others_unchanged()

		p1.x=3; p1.y=2; p1.z=3; p1.save()

		loaded = Coord3D.objects(name="p1")[0]
		self.assertEqual(loaded.x, 3)
		self.assertEqual(loaded.y, 2)
		self.assertEqual(loaded.z, 3)
		test_others_unchanged()

class TestDelete(AbstractDBTest):
	def test_basic_delete(self):
		class Coord3D(Model):
			x = IntField()
			y = IntField()
			z = IntField()

		p1 = Coord3D(); p1.x=1; p1.y=2; p1.z=3; p1.save()
		p2 = Coord3D(); p2.x=1; p2.y=3; p2.z=2; p2.save()
		p3 = Coord3D(); p3.x=3; p3.y=2; p3.z=3; p3.save()

		self.assertEqual(Coord3D.objects().count(), 3)
		p1.delete()
		self.assertEqual(Coord3D.objects().count(), 2)
		self.assertEqual({c._id for c in Coord3D.objects()}, {p2._id, p3._id})

		p2.delete()
		self.assertEqual(Coord3D.objects().count(), 1)
		self.assertEqual(Coord3D.objects()[0]._id, p3._id)

		p3.delete()
		self.assertEqual(Coord3D.objects().count(), 0)


	@unittest.skip("deletion of pointed-to models is not protected under current implementation")
	def test_delete_with_fk(self):
		class Course(Model):
			dept = StringField()
			name = StringField()

		class Student(Model):
			eid = StringField()

		class EnrollmentRecord(Model):
			course = ForeignKeyField(Course)
			enrolled_student = ForeignKeyField(Student)

		chem = Course()
		chem.name = "General Chemistry"
		chem.dept = "Science"
		chem.save()

		stud = Student()
		stud.eid = "1234"
		stud.save()

		enrollment = EnrollmentRecord()
		enrollment.course = chem
		enrollment.enrolled_student = stud 

		enrollment.save()

		chem.delete()
		stud.delete()

		er_loaded = EnrollmentRecord.objects()[0] # will raise an exception bc we deleted its stuff


