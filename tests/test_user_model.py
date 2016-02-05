import unittest

from app.models import User


class UserModelTestCase(unittest.TestCase):
    def test_password_setter(self):
        u = User(password='foo')
        self.assertTrue(u.password_hash is not None)

    def test_no_password_getter(self):
        u = User(password='foo')
        with self.assertRaises(AttributeError):
            u.password