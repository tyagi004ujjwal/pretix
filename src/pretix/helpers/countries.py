from django.core.cache import cache
from django.utils.translation import get_language
from django_countries import Countries


class CachedCountries(Countries):
    _cached_lists = {}

    def __iter__(self):
        """
        Iterate through countries, sorted by name, but cache the results based on the locale.
        django-countries performs a unicode-aware sorting based on pyuca which is incredibly
        slow.
        """
        cache_key = "countries:all:{}".format(get_language())
        if cache_key in self._cached_lists:
            yield from self._cached_lists[cache_key]
            return

        val = cache.get(cache_key)
        if val:
            self._cached_lists[cache_key] = val
            yield from val
            return

        if get_language() == "en":
            countries = self.countries
            countries_first = (self.translate_pair(code) for code in self.countries_first)
            for item in countries_first:
                yield item
            if self.countries_first:
                first_break = self.get_option("first_break")
                if first_break:
                    yield ("", str(first_break))

            # Return sorted country list.
            val = sorted(countries, key=lambda c: c[1])
        else:
            raise ValueError()
            val = list(super().__iter__())

        self._cached_lists[cache_key] = val
        cache.set(cache_key, val, 3600 * 24 * 30)
        yield from val
