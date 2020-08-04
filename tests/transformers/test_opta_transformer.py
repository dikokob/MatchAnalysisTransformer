"""
    Test OPTA transformer
"""
from transformers.SpectrumMatchAnalysisTransformer import SpectrumMatchAnalysisTransformer
from transformers.OPTATransformer import OPTATransformer
import os
import pandas as pd
import json


class TestOptaTransformer:
    """Test the opta transformer
    """
    path_match_results = 'tests/fixtures/raw_data/g1059702/srml-8-2019-f1059702-matchresults.xml'
    path_events = 'tests/fixtures/raw_data/g1059702/f24-8-2019-1059702-eventdetails.xml'
    metadata_path='tests/fixtures/raw_data/g1059702/g1059702_SecondSpectrum_Metadata.json'

    spectrumTransformer = SpectrumMatchAnalysisTransformer('SpectrumMatchAnalysis')

    @staticmethod
    def get_opta_transformers():

        opta_transformer = OPTATransformer()
        return opta_transformer

    def test_output_is_dataframe(self):
        """test that output is a pd.Dataframe
        """

        opta_transformer = self.get_opta_transformers()

        opta_event_data_df, opta_match_info = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        df_player_names_raw, _, _ = self.spectrumTransformer.get_player_match_data(
            match_results_xml=self.path_match_results,
            path_track_meta=self.metadata_path
        )
        output = opta_transformer.opta_shots(
            df_opta_events=opta_event_data_df,
            df_player_names_raw=df_player_names_raw,
            opta_match_info=opta_match_info
        )

        assert (type(output) is pd.DataFrame),(
            "Failed: Output was not of type pd.dataframe"
        )

    def test_output_contains_required_fields(self):
        """test that output contains required fields
        """
        validation_fields = [
            'from_penalty',
            'on_target',
            'off_target'
        ]
        opta_transformer = self.get_opta_transformers()
        opta_event_data_df, opta_match_info = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        df_player_names_raw, _, _ = self.spectrumTransformer.get_player_match_data(
            match_results_xml=self.path_match_results,
            path_track_meta=self.metadata_path
        )
        output = opta_transformer.opta_shots(
            df_opta_events=opta_event_data_df,
            df_player_names_raw=df_player_names_raw,
            opta_match_info=opta_match_info
        )

        for field in validation_fields:
            assert (field in output.columns),(
                "Failed: Output did not contain = {}".format(field)
            )

    def test_get_shots_output_columns_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/processed/g1059702/df_opta_shots.csv", index_col=None)
        opta_transformer = self.get_opta_transformers()
        opta_event_data_df, opta_match_info = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        df_player_names_raw, _, _ = self.spectrumTransformer.get_player_match_data(
            match_results_xml=self.path_match_results,
            path_track_meta=self.metadata_path
        )
        output = opta_transformer.opta_shots(
            df_opta_events=opta_event_data_df,
            df_player_names_raw=df_player_names_raw,
            opta_match_info=opta_match_info
        )

        verification_columns = verification_result.columns
        output_columns = output.columns

        overlap_check = ["missingInOutput:"+c for c in verification_columns if c not in output_columns] + ["missingInVerification:"+c for c in output_columns if c not in verification_columns]
        print(overlap_check)
        assert len(overlap_check) == 0, (
            "Failed: columns did not match up. Missing columns = {}"
            .format(overlap_check)
        )

    def test_get_shots_output_as_expected(self):
        """test output is as expected
        """

        verification_result = pd.read_csv("tests/fixtures/processed/g1059702/df_opta_shots.csv", index_col=None)

        opta_transformer = self.get_opta_transformers()
        opta_event_data_df, opta_match_info = self.spectrumTransformer.get_opta_events(event_path=self.path_events)
        df_player_names_raw, _, _ = self.spectrumTransformer.get_player_match_data(
            match_results_xml=self.path_match_results,
            path_track_meta=self.metadata_path
        )
        output = opta_transformer.opta_shots(
            df_opta_events=opta_event_data_df,
            df_player_names_raw=df_player_names_raw,
            opta_match_info=opta_match_info
        )

        print(verification_result, output)
        # (verification_result.values == output.values).all()
        assert (verification_result.values == output.values).all(), (
            "Failed: did not return required output dataframe"
        )
