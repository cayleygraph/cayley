package path

type NodePath interface {
	Simplify() (NodePath, bool)
	Optimize() (NodePath, bool)
}
type LinkPath interface {
	Simplify() (LinkPath, bool)
	Optimize() (LinkPath, bool)
}
