from dataclasses import dataclass

@dataclass
class ResourceConfiguration:
    cpus: float
    memory: int

    @staticmethod
    def small():
        return ResourceConfiguration(cpus=1, memory=128)
    
    @staticmethod
    def medium():
        return ResourceConfiguration(cpus=2, memory=256)
    
    @staticmethod
    def large():
        return ResourceConfiguration(cpus=4, memory=512)