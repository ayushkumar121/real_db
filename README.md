# real_db: Multi-paradigm Database
=========

## Query Language

### Keywords:

1. Set
2. Insert
2. Select
3. Select_All
4. Range
6. It

### Examples

- Inserting with random Id

```
  "Name" "Ayush" INSERT SELECT
```

- Multiple Insertions with range
```
Range 10 do
  it "Name" "Ayush" Set
  it "Age"  22      Set
End
```
