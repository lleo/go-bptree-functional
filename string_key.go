package bptree

//StringKey is a useful BptKey implementation for strings; construction is
//simply StringKey(mystring)
type StringKey string

//Equals first checks that the argument passed in can be cast to StringKey.
//Then it checks if these two arguments; the reciever and the single argument.
func (k0 StringKey) Equals(K1 BptKey) bool {
	k1, ok := K1.(StringKey)
	if !ok {
		lgr.Printf("incompatable BptKey = %v\n", K1)
		return false
	}
	return string(k0) == string(k1)
}

//LessThan over rules the rules of string comparison (which I think are lame).
//I've come up with a better set of string comparison rules.
//
// StringKey LessThan Rules:
// 0 - shorter is less than longer
// 1 - same sizes are compared as strings
func (k0 StringKey) LessThan(K1 BptKey) bool {
	k1, ok := K1.(StringKey)
	if !ok {
		lgr.Printf("incompatable BptKey = %v\n", K1)
		return false
	}
	if len(k0) < len(k1) {
		return true
	}
	if len(k0) > len(k1) {
		return false
	}
	//len(k0) == len(k1)
	return string(k0) < string(k1)
}

//String Trivial.
func (k StringKey) String() string {
	return string(k)
}
