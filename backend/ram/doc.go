// The ram backend is a testing implementation of an SSI kvl.DB.
//
// It is not fast (especially in the case of range queries), but it is correct
// and can be used to test correctness of any other backend that should
// implement serializable snapshot isolation.
package ram
