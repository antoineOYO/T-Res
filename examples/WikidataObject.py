import requests
import json
from geopy.distance import distance

class WikidataObject:
    def __init__(self, uri=None, jsondata=None, label=None, coordinates=None,
                 outgoing_edges=None, nb_statements=None, nb_sitelinks=None,
                 types=None, aliases=None):
        self.uri = self._process_uri(uri)
        self.label = label
        self.coordinates = coordinates
        self.json = jsondata
        self.link = f'https://www.wikidata.org/wiki/{self.uri}' if self.uri else None
        self.outgoing_edges = outgoing_edges
        self.nb_statements = nb_statements
        self.nb_sitelinks = nb_sitelinks
        self.types = types
        self.aliases = aliases

    def _process_uri(self, uri):
        if uri is None:
            return None
        if isinstance(uri, (int, float)):
            return 'Q' + str(int(uri))
        elif isinstance(uri, str):
            if uri.startswith('Q'):
                return uri
            else:
                raise ValueError("URI should start with 'Q' if it's a string.")
        else:
            raise ValueError("URI must be a string or numeric value.")

    def __repr__(self):
        return self.link

    def __iter__(self):
        return iter(self.json) if self.json else iter([])

    def _request_json(self):
        if self.uri is None:
            raise AttributeError("URI is not set.")
        try:
            url = f'https://www.wikidata.org/wiki/Special:EntityData/{self.uri}.json'
            response = requests.get(url)
            response.raise_for_status()  # Raise HTTPError for bad response status
            data = response.json()
            self.json = data
            return data
        except requests.RequestException as e:
            print(f'Error requesting JSON at URL: {url}')
            print(e)
            return None

    # def _get_label(self, lang='fr'):
    #     """
    #     default lang = fr
    #     if a language is provide,
    #         returns the label (one !) of the item in the given language
    #     if lang='all',
    #         returns a dict of all labels 
    #     """
    #     if self.label:
    #         return self.label
    #     if self.json:
    #         if lang == 'all':
    #             try:
    #                 return self.json['entities'][self.uri]['labels']
    #             except KeyError:
    #                 pass
    #         else:
    #             try:
    #                 return self.json['entities'][self.uri]['labels'][lang]['value']
    #             except KeyError:
    #                 pass
    #     json_data = self._request_json()
    #     if json_data:
    #         return self._get_label()
    #     return None
    
    def _get_label(self, lang='fr'):
        """
        default lang = fr
        if a language is provided,
            returns the label (one!) of the item in the given language
        if lang='all',
            returns a dict of all labels 
        """
        if self.label:
            return self.label
        
        if not self.json:
            self.json = self._request_json()
        
        entity_info = self.json.get('entities', {}).get(self.uri, {})
        labels = entity_info.get('labels', {})

        if lang == 'all':
            self.label = {l: l_dict['value'] for l, l_dict in labels.items()}
            return self.label
        
        # else :
        self.label = labels.get(lang, {}).get('value', None)
        return self.label

            
    def _get_outgoing_edges(self, include_p31=True, numeric=True):
        """
        Given a JSON representation of an item,
        return the list of outgoing edges,
        as integers.
        """
        if self.outgoing_edges:
            return self.outgoing_edges
        
        if not self.json:
            json_data = self._request_json()
            if json_data:
                return self._get_outgoing_edges()
        else:
            try:
                claims = self.json['entities'][self.uri]['claims']
                final_key = 'numeric-id' if numeric else 'id'
                res = []
                for pid, pclaims in claims.items():
                    if pid == 'P31' and not include_p31:
                        continue
                    for c in pclaims:
                        try:
                            res.append(c['mainsnak']['datavalue']['value'][final_key])
                        except (KeyError, TypeError):
                            pass

                        qualifiers = c.get('qualifiers', {})
                        for pid, qs in qualifiers.items():
                            for q in qs:
                                try:
                                    res.append(q['datavalue']['value'][final_key])
                                except (KeyError, TypeError):
                                    pass
                self.outgoing_edges = res
                return res
            
            except Exception as e:
                print('Error getting outgoing edges:', e)
                return None
        

    def _get_nb_statements(self):
        """
        Number of claims on the item
        """
        if self.nb_statements:
            return self.nb_statements
        
        if not self.json:
            json_data = self._request_json()
            if json_data:
                return self._get_nb_statements()
        else:
            try:
                nb_claims = 0
                for pclaims in self.json['entities'][self.uri]['claims'].values():
                    nb_claims += len(pclaims)
                self.nb_statements = nb_claims    
                return nb_claims
            except Exception as e:
                print('Error getting number of statements:', e)
                return None

    def _get_nb_sitelinks(self):
        """
        Number of sitelinks on this item
        """
        if self.nb_sitelinks:
            return self.nb_sitelinks
        
        if not self.json:
            json_data = self._request_json()
            if json_data:
                return self._get_nb_sitelinks()
            
        else:
            try:
                nb_sitelinks = len(self.json['entities'][self.uri]['sitelinks'])
                self.nb_sitelinks = nb_sitelinks
                return nb_sitelinks
            except Exception as e:
                print('Error getting number of sitelinks:', e)
                return None
       
    def _get_types(self, pid='P31'):
        """
        Values of P31 claims
        """
        if self.types:
            return self.types
        
        if not self.json:
            self.json = self._request_json()
            if not self.json:
                return None
        
        try:
            type_claims = self.json['entities'][self.uri]['claims'].get(pid, [])
            valid_type_qids = [claim['mainsnak']['datavalue']['value']['id'] for claim in type_claims if claim.get('mainsnak')]
            self.types = valid_type_qids
            return valid_type_qids
        except Exception as e:
            print('Error getting types:', e)
            return None

    def _get_aliases(self, lang='fr'):
        """
        default lang = fr
        if a language is provided,
            returns the aliases of the item in the given language
        if lang='all',
            returns a dict of all aliases 
        """
        if self.aliases:
            return self.aliases
        
        if not self.json:
            self.json = self._request_json()
            if not self.json:
                return None
            
        entity_info = self.json.get('entities', {}).get(self.uri, {})
        aliases_info = entity_info.get('aliases', {})

        if lang == 'all':
            aliases = {}
            for lang, lang_aliases in aliases_info.items():
                aliases[lang] = [alias['value'] for alias in lang_aliases]
            self.aliases = aliases
            return aliases

        # else :
        lang_aliases = aliases_info.get(lang, [])
        aliases = [alias['value'] for alias in lang_aliases]
        self.aliases = aliases
        return aliases

    def _get_coordinates(self):
        if isinstance(self.coordinates, (tuple, list)) and len(self.coordinates) == 2:
            return self.coordinates

        if not self.json:
            self.json = self._request_json()
            if not self.json:
                return None

        try:
            coordinates_claim = self.json['entities'][self.uri]['claims'].get('P625', [])
            if coordinates_claim:
                coordinates_dict = coordinates_claim[0]['mainsnak']['datavalue']['value']
                self.coordinates = (coordinates_dict['latitude'], coordinates_dict['longitude'])
                return self.coordinates
            else:
                return None
        except Exception as e:
            print('Error getting coordinates:', e)
            return None
            
    def _distance_to(self, other):
        """
        other should be a WikidataObject
        or a tuple of coordinates (lat, lon)
        or a list of coordinates [lat, lon]
        or a string or int of a Wikidata URI.

        returns the distance in km between the two objects.
        """

        self_coords = self._get_coordinates()

        if isinstance(other, (int, str, float)):
            other = WikidataObject(uri=other)

        if isinstance(other, WikidataObject):
            other_coords = other._get_coordinates()
        elif isinstance(other, (tuple, list)) and len(other) == 2:
            other_coords = other
        else:
            return None

        if self_coords and other_coords:
            return distance(self_coords, other_coords).km
        else:
            return None
        
    def _to_dict(self, fulfill=False):
        """
        fulfill: if True, will fetch missing data from Wikidata
        the dict is of the form
        {
            'uri': self.uri,
            'link': self.link,
            'coordinates': self.coordinates,
            'outgoing_edges': self.outgoing_edges,
            'nb_statements': self.nb_statements,
            'nb_sitelinks': self.nb_sitelinks,
            'types': self.types,
            'aliases': self.aliases
        }
        """
        data = {
            'uri': self.uri,
            'link': self.link,
            'coordinates': self.coordinates,
            'outgoing_edges': self.outgoing_edges,
            'nb_statements': self.nb_statements,
            'nb_sitelinks': self.nb_sitelinks,
            'types': self.types,
            'aliases': self.aliases
        }

        if fulfill:
            data['label'] = self._get_label()
            data['coordinates'] = self._get_coordinates()
            data['outgoing_edges'] = self._get_outgoing_edges() 
            data['nb_statements'] = self._get_nb_statements() 
            data['nb_sitelinks'] = self._get_nb_sitelinks()
            data['types'] = self._get_types()
            data['aliases'] = self._get_aliases()

        return data

