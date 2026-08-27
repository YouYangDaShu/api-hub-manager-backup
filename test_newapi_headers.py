import unittest

from services.newapi import NewAPIAdapter


class NewAPIHeaderTests(unittest.TestCase):
    def test_standard_newapi_uses_new_api_user_header(self):
        adapter = NewAPIAdapter("https://example.com", "session=value", "cookie")
        adapter.user_id = "42"

        self.assertEqual(adapter._headers()["New-Api-User"], "42")
        self.assertNotIn("Huiliu-Api-User", adapter._headers())

    def test_huiliu_uses_its_required_user_header(self):
        adapter = NewAPIAdapter("https://api.huiliu.one", "session=value", "cookie")
        adapter.user_id = "42"

        self.assertEqual(adapter._headers()["Huiliu-Api-User"], "42")
        self.assertNotIn("New-Api-User", adapter._headers())


if __name__ == "__main__":
    unittest.main()
