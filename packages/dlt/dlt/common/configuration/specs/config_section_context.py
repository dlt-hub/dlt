from typing import Callable, List, Optional, Tuple
from dlt.common.configuration.specs import known_sections

from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext, configspec


@configspec
class ConfigSectionContext(ContainerInjectableContext):
    TMergeFunc = Callable[["ConfigSectionContext", "ConfigSectionContext"], None]

    pipeline_name: Optional[str] = None
    sections: Tuple[str, ...] = ()
    merge_style: TMergeFunc = None
    source_state_key: str = None

    def merge(self, existing: "ConfigSectionContext") -> None:
        """Merges existing context into incoming using a merge style function"""
        merge_style_f = self.merge_style or self.prefer_incoming
        merge_style_f(self, existing)

    def source_name(self) -> str:
        """Gets name of a source from `sections`"""
        if self.sections and len(self.sections) == 3 and self.sections[0] == known_sections.SOURCES:
            return self.sections[-1]
        raise ValueError(self.sections)

    def source_section(self) -> str:
        """Gets section of a source from `sections`"""
        if self.sections and len(self.sections) == 3 and self.sections[0] == known_sections.SOURCES:
            return self.sections[1]
        raise ValueError(self.sections)

    @staticmethod
    def prefer_incoming(incoming: "ConfigSectionContext", existing: "ConfigSectionContext") -> None:
        incoming.pipeline_name = incoming.pipeline_name or existing.pipeline_name
        incoming.sections = incoming.sections or existing.sections
        incoming.source_state_key = incoming.source_state_key or existing.source_state_key

    @staticmethod
    def prefer_existing(incoming: "ConfigSectionContext", existing: "ConfigSectionContext") -> None:
        """Prefer existing section context when merging this context before injecting"""
        incoming.pipeline_name = existing.pipeline_name or incoming.pipeline_name
        incoming.sections = existing.sections or incoming.sections
        incoming.source_state_key = existing.source_state_key or incoming.source_state_key

    @staticmethod
    def resource_merge_style(
        incoming: "ConfigSectionContext", existing: "ConfigSectionContext"
    ) -> None:
        """If top level section is same and there are 3 sections it replaces second element (source module) from existing and keeps the 3rd element (name)"""
        incoming.pipeline_name = incoming.pipeline_name or existing.pipeline_name
        if (
            len(incoming.sections) == 3 == len(existing.sections)
            and incoming.sections[0] == existing.sections[0]
        ):
            # existing does not have middle section then keep incoming
            incoming.sections = (
                incoming.sections[0],
                existing.sections[1] or incoming.sections[1],
                incoming.sections[2],
            )
            incoming.source_state_key = existing.source_state_key or incoming.source_state_key
        else:
            incoming.sections = incoming.sections or existing.sections
            incoming.source_state_key = incoming.source_state_key or existing.source_state_key

    def __str__(self) -> str:
        return (
            super().__str__()
            + f": {self.pipeline_name} {self.sections}@{self.merge_style} state['{self.source_state_key}']"
        )
