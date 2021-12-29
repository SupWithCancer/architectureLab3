from flask_restful import reqparse

class Specification:

    def __and__(self, other):
        return And(self, other)

    def __or__(self, other):
        return Or(self, other)

    def __xor__(self, other):
        return Xor(self, other)

    def __invert__(self):
        return Invert(self)

    def is_satisfied_by(self, candidate):
        raise NotImplementedError()

    def remainder_unsatisfied_by(self, candidate):
        if self.is_satisfied_by(candidate):
            return None
        else:
            return self


class CompositeSpecification(Specification):
    pass


class MultaryCompositeSpecification(CompositeSpecification):

    def __init__(self, *specifications):
        self.specifications = specifications


class And(MultaryCompositeSpecification):

    def __and__(self, other):
        if isinstance(other, And):
            self.specifications += other.specifications
        else:
            self.specifications += (other, )
        return self

    def is_satisfied_by(self, candidate):
        satisfied = all([
            specification.is_satisfied_by(candidate)
            for specification in self.specifications
        ])
        return satisfied

    def remainder_unsatisfied_by(self, user):
        non_satisfied = [
            specification
            for specification in self.specifications
            if not specification.is_satisfied_by(user)
        ]
        if not non_satisfied:
            return None
        if len(non_satisfied) == 1:
            return non_satisfied[0]
        if len(non_satisfied) == len(self.specifications):
            return self
        return And(*non_satisfied)


class Or(MultaryCompositeSpecification):

    def __or__(self, other):
        if isinstance(other, Or):
            self.specifications += other.specifications
        else:
            self.specifications += (other, )
        return self

    def is_satisfied_by(self, user):
        satisfied = any([
            specification.is_satisfied_by(user)
            for specification in self.specifications
        ])
        return satisfied


class UnaryCompositeSpecification(CompositeSpecification):

    def __init__(self, specification):
        self.specification = specification


class Invert(UnaryCompositeSpecification):

    def is_satisfied_by(self, user):
        return not self.specification.is_satisfied_by(user)


class BinaryCompositeSpecification(CompositeSpecification):

    def __init__(self, left, right):
        self.left = left
        self.right = right


class Xor(BinaryCompositeSpecification):

    def is_satisfied_by(self, user):
        return (
            self.left.is_satisfied_by(user) ^
            self.right.is_satisfied_by(user)
        )


class NullaryCompositeSpecification(CompositeSpecification):
    pass


class TrueSpecification(NullaryCompositeSpecification):

    def is_satisfied_by(self, user):
        return True


class FalseSpecification(NullaryCompositeSpecification):

    def is_satisfied_by(self, user):
        return False

class VaccancyName(Specification):
  def is_satisfied_by(self, vaccancy):
      parser = reqparse.RequestParser()
      parser.add_argument("vaccancy_name")
      args = parser.parse_args()
      if args['vaccancy_name']:
          return vaccancy['vaccancy_name'] == args['vaccancy_name']
      else:
          return True
class MinSalary(Specification):
  def is_satisfied_by(self, vaccancy):
      parser = reqparse.RequestParser()
      parser.add_argument("min_salary")
      args = parser.parse_args()
      if args['min_salary']:
          return vaccancy['salary'] > int(args['min_salary'])
      else:
          return True
class MaxSalary(Specification):
  def is_satisfied_by(self, vaccancy):
      parser = reqparse.RequestParser()
      parser.add_argument("max_salary")
      args = parser.parse_args()
      if args['max_salary']:
          return vaccancy['salary'] < int(args['max_salary'])
      else:
          return True
