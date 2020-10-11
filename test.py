from Database import Datastore, DerivedSettings
from transformScript import Function, Expression, ExpressionValue, FunctionOperators, ValueTypes, Union
database = Datastore()

database.add_source_table("Source1")
database.add_source_table("Source2")
addOne = Function([Expression(FunctionOperators.ADD,ExpressionValue(ValueTypes.FIELD, "foo"), ExpressionValue(ValueTypes.SCALAR,value=1), "foo")],"Source1", "Added")
settingsAddOne = DerivedSettings(["Source1"], [], transform=addOne)

union = Union(["Source2", "Added"], "Union")
settingsUnion = DerivedSettings(["Source2", "Added"], [], transform=union)



database.add_derived_table("Added", settingsAddOne)
database.add_derived_table("Union", settingsUnion)


database.add_data("Source1", "V1", {"foo": 1, "bar": "fizz"})
database.add_data("Source2", "V1", {"Something": "Completely", "Different": 12.3})

print(database.get_table("Source1"))
print(database.get_table("Source2"))
print(database.get_table("Added"))
print(database.get_table("Union"))