.. py:currentmodule:: mdf

Class Nodes
===========

Node definitions may also be applied to python classes. An :py:func:`evalnode`
when declared on a class applies to either a ``classmethod`` or a ``staticmethod``.

Nodes may not apply to instance methods as state should be maintained
in the DAG and not in class instances, and so nodes only make sense in
the context of class methods and static methods.

::

    from mdf import MDFContext, evalnode, varnode

    class MyClass(object):

        # varnodes may be declared as class attributes
        a = varnode(default=10)
        b = varnode(default=20)
    
        # evalnodes may be declared using class methods
        @evalnode
        def function_of_a_and_b(cls):
            # as this is a classmethod it has access to other nodes on
            # the class, the same way as it can access any other class
            # attributes or methods.
            return cls.a() * cls.b()

        @evalnode
        def example_node(cls):
            return cls.function_of_a_and_b() * 5

Class nodes are referenced and used in exactly the same way as non-class
nodes::

    ctx = MDFContext()
    
    # class nodes are attributes of their class and are referenced in the normal way
    ctx[MyClass.a] = 5
    ctx[MyClass.b] = 10
    
    print clx[MyClass.example_node]

Class nodes behave just like normal python class methods with respect
to inheritance. Class nodes may be overridden or re-used by subclasses
in the same way as class methods::

    class MyDerivedClass(MyClass):

        @evalnode
        def function_of_a_and_b(cls):
            return cls.a() + b()

    print ctx[MyDerivedClass.example_node] 

In the example above, :py:func:`MyDerivedClass.example_node` is a node inherited from the
base class ``MyClass`` which calls :py:func:`cls.function_of_a_and_b`. As that
was overridden in the derived class that overridden implementation will be used
when evaluating :py:func:`MyDerivedClass.example_node`.  

Class :py:func:`evalnode` nodes (and derived node types) are bound to their
owning classes. This means that when a node is referenced by one class it
is a distinct node from when the same node definition is referenced from
a derived class. In the example above both ``MyClass`` and
``MyDerivedClass`` have :py:func:`example_node` nodes. Even though it is
only declared on ``MyClass`` and inherited by ``MyDerivedClass``
:py:func:`MyClass.example_node` and :py:func:`MyDerivedClass.example_node` are
different nodes in the DAG.

:py:func:`varnode` nodes are not bound in the same way as they are just
class attributes and so :py:func:`MyClass.a` and :py:func:`MyDerivedClass.a`
actually refer to exactly the same object, and so they are the same nodes
in the DAG. It is possible to override class attributes in Python though,
and so :py:func:`varnode` nodes may be overriden in the same way.
