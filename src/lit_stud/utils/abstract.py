from io import StringIO
from html.parser import HTMLParser
import re

CLEANR = re.compile('<.*?>') 
# CLEANR2 = re.compile('\&[0-9A-Fa-f\&]*?;') 
CLEANR2 = re.compile('\&[a-zA-Z0-9;&/#]*;') 

def cleanhtml(raw_html):
    cleantext = re.sub(CLEANR, '', raw_html)
    cleantext = re.sub(CLEANR2, '', cleantext)
    cleantext = cleantext.replace("  ", " ")
    cleantext = cleantext.replace("\n\n", "\n")
    return cleantext

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = StringIO()

    def handle_data(self, d):
        self.text.write(d)

    def get_data(self):
        return self.text.getvalue()

def strip_tags(html):

    return cleanhtml(html)
    # s = MLStripper()
    # s.feed(html)
    # return s.get_data()


# <jats:p>&amp;lt;div&amp;gt;
# &amp;lt;div&amp;gt;
# &amp;lt;p&amp;gt;Considering climate change, it is essential to reduce CO&amp;lt;sub&amp;gt;2&amp;lt;/sub&amp;gt; emissions. The provision of charging infrastructure in public spaces for electromobility &amp;amp;#8211; along with the substitution of conventional power generation with renewable energies &amp;amp;#8211; can contribute to the energy transition in the transport sector. Scenarios for the spatial distribution of this charging infrastructure can help to exemplify the need for charging points and their impact, for example, on power grids. We present an approach based both on the usage frequency of points of interest (POIs) and on the need for charging points in residential areas. This approach is validated in several steps and compared with alternative methods, such as a machine learning model trained with existing charging point utilization data.&amp;lt;/p&amp;gt;
# &amp;lt;p&amp;gt;Our approach uses two drivers to model the demand for public charging infrastructure. The first driver represents the demand for more charging stations to compensate for the lack of home charging stations and is derived from a previously developed and published model addressing electric-vehicle ownership (with and without home charging options) in households. The second driver represents the demand for public charging infrastructure at POIs. Their locations are derived from Open Street Map (OSM) data and weighted based on an evaluation of movement profiles from the Mobilit&amp;amp;#228;t in Deutschland survey (MiD, German for &amp;amp;#8220;Mobility in Germany&amp;amp;#8221;). We combine those two drivers with the available parking spaces and generate distributions for possible future charging points. For computational efficiency and speed, we use a raster-based approach in which all vector data is rasterized and computations are performed on the full grid of a municipality. The presented application area is Wiesbaden, Germany, and the methodology is generally applicable to municipalities in Germany.&amp;lt;/p&amp;gt;
# &amp;lt;p&amp;gt;The method is compared and validated with alternative approaches on several levels. First, the allocation of parking space based on the raster calculation is validated against parking space numbers available in OSM. Second, the modeling of charging points supposed to compensate for the lack of home charging opportunities is contrasted with a simplified procedure by means of an analysis of multifamily housing density. In the third validation step, the method is compared to an existing machine learning model that estimates spatial suitability for charging stations. This model is trained with numerous input datasets such as population density and POIs on the one hand and utilization data of existing charging stations on the other hand. The objective of these comparisons is both to generally verify our model&amp;amp;#8217;s validity and to investigate the relative influence of specific components of the model.&amp;lt;/p&amp;gt;
# &amp;lt;p&amp;gt;The identification of potential charging points in public spaces plays an important role in modeling the future energy system &amp;amp;#8211; especially the power grid &amp;amp;#8211; as the rapid adoption of electric vehicles will shift locations of demand for electricity. With our investigation, we want to present a new method to simulate future public charging point locations and show the influences of different modeling methods.&amp;lt;/p&amp;gt;
# &amp;lt;/div&amp;gt;
# &amp;lt;/div&amp;gt;</jats:p>
